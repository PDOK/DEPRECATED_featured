(ns pdok.featured.timeline
  (:refer-clojure :exclude [merge])
  (:require [pdok.cache :refer :all]
            [pdok.postgres :as pg]
            [pdok.featured.projectors :as proj]
            [pdok.featured.persistence :as pers]
            [pdok.featured.tiles :as tiles]
            [clojure.java.jdbc :as j]
            [clojure.core.cache :as cache]
            [clojure.string :as str]
            [clj-time.core :as t]))

(defn indices-of [f coll]
  (keep-indexed #(if (f %2) %1 nil) coll))

(defn index-of [f coll]
  (first (indices-of f coll)))

(def ^:dynamic *timeline-schema* "featured")
(def ^:dynamic *history-table* "timeline")
(def ^:dynamic *current-table* "timeline_current")

(defn- create-history-table [db]
  "Create table with default fields"
  (pg/create-table db *timeline-schema* *history-table*
               [:id "serial" :primary :key]
               [:dataset "varchar(100)"]
               [:collection "varchar(255)"]
               [:feature_id "varchar(100)"]
               [:version "uuid"]
               [:valid_from "timestamp without time zone"]
               [:valid_to "timestamp without time zone"]
               [:feature "text"]
               [:tiles "integer[]"])
  (pg/create-index db *timeline-schema* *history-table* :dataset :collection :feature_id)
  (pg/create-index db *timeline-schema* *history-table* :version))

(defn- create-current-table [db]
  (pg/create-table db *timeline-schema* *current-table*
               [:id "serial" :primary :key]
               [:dataset "varchar(100)"]
               [:collection "varchar(255)"]
               [:feature_id "varchar(100)"]
               [:version "uuid"]
               [:all_versions "uuid[]"]
               [:valid_from "timestamp without time zone"]
               [:valid_to "timestamp without time zone"]
               [:feature "text"]
               [:tiles "integer[]"])
  (pg/create-index db *timeline-schema* *current-table* :dataset :collection :feature_id)
  (pg/create-index db *timeline-schema* *current-table* :version))

(defn- init [db]
  (when-not (pg/schema-exists? db *timeline-schema*)
    (pg/create-schema db *timeline-schema*))
  (when-not (pg/table-exists? db *timeline-schema* *history-table*)
    (create-history-table db))
  (when-not (pg/table-exists? db *timeline-schema* *current-table*)
    (create-current-table db)))

(defn- qualified-history []
  (str (name *timeline-schema*) "." (name *history-table*)))

(defn- qualified-current []
  (str (name *timeline-schema*) "." (name *current-table*)))

(defn current [db dataset collection]
  (j/with-db-connection [c db]
    (let [results
          (j/query c [(str "SELECT tiles, feature FROM " (qualified-current)
                           " WHERE dataset = ? AND collection = ? AND valid_to is null")
                      dataset collection] :as-arrays? true)
          current (map (fn [[t f]] (vector t (pg/from-json f))) (drop 1 results))]
      current)))

(defn closed [db dataset collection]
  (j/with-db-connection [c db]
    (let [results
          (j/query c [(str "SELECT tiles, feature FROM " (qualified-current)
                           " WHERE dataset = ? AND collection = ? AND valid_to is not null")
                      dataset collection] :as-arrays? true)
          current (map (fn [[t f]] (vector t (pg/from-json f))) (drop 1 results))]
      current)))

(defn history [db dataset collection]
  (j/with-db-connection [c db]
    (let [results
          (j/query c [(str "SELECT tiles, feature FROM " (qualified-history)
                           " WHERE dataset = ? AND collection = ?")
                      dataset collection] :as-arrays? true)
          current (map (fn [[t f]] (vector t (pg/from-json f))) (drop 1 results))]
      current)))

(defn- init-root
  ([feature]
   (if-let [dataset (:dataset feature)]
     (init-root dataset (:collection feature) (:id feature))
     (init-root (:collection feature) (:id feature))))
  ([collection id]
   {:_collection collection :_id id })
  ([dataset collection id]
   (assoc (init-root collection id) :_dataset dataset)))

(defn- mustafy [feature]
  (let [result (init-root feature)
        geom (:geometry feature)
        result (cond-> result geom (assoc :_geometry geom))
        result (reduce (fn [acc [k v]] (assoc acc (keyword k) v)) result (:attributes feature))]
    result))

(defn- path->merge-fn [target path]
  (if-not (empty? path)
    (loop [[[field id] & rest] path
           t target
           f identity]
      ;(println field id t f)
      (if field
        (let [field-value (get target field)]
          (if field-value
            (cond
             (vector? field-value)
             (if-let [i (index-of #(= id (:_id %)) field-value)]
               (recur rest (get field-value i) #(f (update-in t [field i] (fn [v] (clojure.core/merge v %)))))
               (do ;(println "vector: not found")
                   (recur rest {} #(f (assoc-in t [field (count field-value)] %)))))
             ;; if it is not a vector we are probable replacing existing values.
             ;; Treat this as a non existing value, ie. override value;
             :else (do ;(println "override:" field (f {}))
                       (recur rest (init-root field id) #(f (assoc t field (vector %)))))
             )
            ;; no field-value => we are working with child objects, place it in a vector
            (do ;(println "no-value: " field (f {}))
                (recur rest nil #(f (assoc t field (vector %))))
              )
            ))
        (do ;(println "no path: "(f {}))
            f
          )))
    #(clojure.core/merge target %))
  )

(defn- merge
  ([target path feature]
   (let [keyworded-path (map (fn [[_ id field]] [(keyword field) id]) path)
         mustafied (mustafy feature)
         merger (path->merge-fn target keyworded-path)
         merged (merger mustafied)
         merged (assoc merged :_version (:version feature))
         merged (update-in merged [:_all_versions] (fnil conj '()) (:version feature))
         merged (update merged :_tiles #((fnil clojure.set/union #{}) % (tiles/nl (:geometry feature))))]
     merged)))

(defn- sync-valid-from [acc feature]
  (assoc acc :_valid_from  (:validity feature)))

(defn- sync-valid-to [acc feature]
  (assoc acc :_valid_to (:validity feature)))

(defn- new-current-sql []
  (str "INSERT INTO " (qualified-current)
       " (dataset, collection, feature_id, version, all_versions, valid_from, valid_to, feature, tiles)
VALUES (?, ?, ? ,?, ?, ?, ?, ?, ?)"))

(defn- new-current
  ([db features]
   (try
     (let [transform-fn (juxt :_dataset :_collection :_id :_version :_all_versions
                              :_valid_from :_valid_to pg/to-json :_tiles)
           records (map transform-fn features)]
       (j/execute! db (cons (new-current-sql) records) :multi? true :transaction? false))
      (catch java.sql.SQLException e (j/print-sql-exception-chain e))))
  )

;; (defn- get-current [db dataset collection id]
;;   (j/with-db-connection [c db]
;;     (let [results
;;           (j/query c [(str "SELECT  FROM " (qualified-current)
;;                            " WHERE dataset = ? AND collection = ?  AND feature_id = ?")
;;                       dataset collection id])]
;;       (pg/from-json (:feature (first results))))))

(defn- load-current-feature-cache-sql []
  (str "SELECT dataset, collection, feature_id, feature  FROM " (qualified-current)
   " WHERE dataset = ? AND collection = ?")
  )

(defn- load-current-feature-cache [db dataset collection]
  (j/with-db-connection [c db]
    (let [results
          (j/query c [(load-current-feature-cache-sql)
                      dataset collection] :as-arrays? true)
          for-cache
          (map (fn [[ds col fid f]] [ [ds col fid]] (pg/from-json f) ) (drop 1 results))]
      for-cache)))

(defn- update-current-sql []
  (str "UPDATE " (qualified-current) "
SET feature = ?,
    version = ?,
    all_versions = ?,
    valid_from = ?,
    valid_to = ?,
    tiles = ?
WHERE dataset = ? AND collection = ? AND  feature_id = ?"))

(defn- update-current [db features]
  (try
    (let [transform-fn (juxt pg/to-json :_version :_all_versions :_valid_from :_valid_to
                             :_tiles :_dataset :_collection :_id)
          records (map transform-fn features)]
      (j/execute! db (cons (update-current-sql) records) :multi? true :transaction? false))
    (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(defn- delete-current-sql []
  (str "DELETE FROM " (qualified-current)
       " WHERE version = ?"))

(defn- delete-current [db versions]
  (try
    (let [records (map #(vector %) versions)]
      (j/execute! db (cons (delete-current-sql) records) :multi? true :transaction? false))
    (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(defn- new-history-sql []
  (str "INSERT INTO " (qualified-history) " (dataset, collection, feature_id, version, valid_from, valid_to, feature, tiles)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)"))

(defn- new-history [db features]
  (try
    (let [transform-fn (juxt :_dataset :_collection :_id :_version :_valid_from :_valid_to pg/to-json :_tiles)
          records (map transform-fn features)]
      (j/execute! db (cons (new-history-sql) records) :multi? true :transaction? false))
    (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(defn- delete-history-sql []
  (str "DELETE FROM " (qualified-history)
       " WHERE ARRAY[version] <@ ?"))

(defn- delete-history [db features]
  (try
    (let [records (map (juxt :_all_versions) features)]
      (j/execute! db (cons (delete-history-sql) records) :multi? true :transaction? false))
    (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(defn- feature-key [feature]
  [(:dataset feature) (:collection feature) (:id feature)])

(defn- flush-all [db new-current-batch delete-current-batch new-history-batch delete-history-batch]
  "Used for flushing all batches, so update current is always performed after new current"
  (flush-batch new-current-batch (partial new-current db))
  (flush-batch delete-current-batch (partial delete-current db))
  (flush-batch new-history-batch (partial new-history db))
  (flush-batch delete-history-batch (partial delete-history db)))

(defn- cache-store-key [f]
  [(:_dataset f) (:_collection f) (:_id f)])

(defn- cache-value [_ f] f)

(defn- cache-invalidate [_ f])

(defn- cache-use-key [dataset collection id]
  [dataset collection id])

(defn- batched-updater [db ncb ncb-size dcb dcb-size flush-fn feature-cache]
  "Cache batched updater consisting of insert and delete"
  (let [batched-new (with-batch ncb ncb-size (partial new-current db) flush-fn)
        cache-batched-new (with-cache feature-cache batched-new cache-store-key cache-value)
        batched-delete (with-batch dcb dcb-size (partial new-current db) flush-fn)]
    (fn [f]
      (cache-batched-new f)
      (batched-delete (first (rest (:_all_versions f)))))))

(defn- batched-deleter [db dcb dcb-size dhb dhb-size flush-fn feature-cache]
  "Cache batched deleter (thrasher) of current and history"
  (let [batched-delete-cur (with-batch dcb dcb-size (partial delete-current db) flush-fn)
        cache-batched-delete-cur (with-cache feature-cache batched-delete-cur cache-store-key cache-invalidate)
        batched-delete-his (with-batch dhb dhb-size (partial delete-history db) flush-fn)]
    (fn [f]
      (cache-batched-delete-cur (:_version f))
      (batched-delete-his f))))

(deftype Timeline [db root-fn path-fn
                   feature-cache cache-loader
                   new-current-batch new-current-batch-size
                   delete-current-batch delete-current-batch-size
                   new-history-batch new-history-batch-size
                   delete-history-batch delete-history-batch-size
                   flush-fn]
  proj/Projector
  (init [this]
    (init db) this)
  (proj/new-feature [_ feature]
    (let [[dataset collection id] (feature-key feature)
          [root-col root-id] (root-fn dataset collection id)
          path (path-fn dataset collection id)
          batched-new (with-batch new-current-batch new-current-batch-size (partial new-current db) flush-fn)
          cache-batched-new (with-cache feature-cache batched-new cache-store-key cache-value)
          cache-batched-update (batched-updater db new-current-batch new-current-batch-size
                                          delete-current-batch delete-current-batch-size
                                          flush-fn feature-cache)
          batched-history (with-batch new-history-batch new-history-batch-size (partial new-history db) flush-fn)
          cached-get-current (use-cache feature-cache cache-use-key cache-loader)]
      (if-let [current (cached-get-current dataset root-col root-id)]
        (let [new-current (merge current path feature)]
          (if (= (:action feature) :close)
            (cache-batched-update (sync-valid-to new-current feature))
            (if (t/before? (:_valid_from current) (:validity feature))
              (do ;(println "NOT-SAME")
                (batched-history (sync-valid-to current feature))
                (cache-batched-update (sync-valid-from new-current feature)))
              (do ;(println "SAME")
                (cache-batched-update (sync-valid-from new-current feature))))))
        (cache-batched-new (sync-valid-from (merge (init-root dataset root-col root-id) path feature) feature)))))
  (proj/change-feature [_ feature]
    ;; change can be the same, because a new nested feature validity change will also result in a new validity
    ;(println "CHANGE")
    (proj/new-feature _ feature))
  (proj/close-feature [_ feature]
    (proj/new-feature _ feature))
  (proj/delete-feature [_ feature]
    (let [[dataset collection id] (feature-key feature)
          [root-col root-id] (root-fn dataset collection id)
          cache-batched-delete (batched-deleter db delete-current-batch delete-current-batch-size
                                                delete-history-batch delete-history-batch-size
                                                flush-fn feature-cache)
          cached-get-current (use-cache feature-cache cache-use-key cache-loader)]
      (when-let [current (cached-get-current dataset root-col root-id)]
        (cache-batched-delete current))))
  (proj/close [this]
    (flush-fn)
    this)
  )

(defn create [config]
  (let [db (:db-config config)
        persistence (or (:persistence config) (pers/cached-jdbc-processor-persistence config))
        cache (ref (cache/basic-cache-factory {}))
        load-cache?-fn (once-true-fn)
        cache-loader (fn [dataset collection id]
                       (when (load-cache?-fn dataset collection)
                         (load-current-feature-cache db dataset collection)))
        new-current-batch-size (or (:new-current-batch-size config) (:batch-size config) 10000)
        new-current-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        delete-current-batch-size (or (:update-current-batch-size config) (:batch-size config) 10000)
        delete-current-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        new-history-batch-size (or (:new-history-batch-size config) (:batch-size config) 10000)
        new-history-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        delete-history-batch-size (or (:delete-history-batch-size config) (:batch-size config) 10000)
        delete-history-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        flush-fn #(flush-all db new-current-batch delete-current-batch new-history-batch delete-history-batch)]
    (->Timeline db (partial pers/root persistence) (partial pers/path persistence)
                cache cache-loader
                new-current-batch new-current-batch-size
                delete-current-batch delete-current-batch-size
                new-history-batch new-history-batch-size
                delete-history-batch delete-history-batch-size
                flush-fn)))
