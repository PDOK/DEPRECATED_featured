(ns pdok.featured.timeline
  (:refer-clojure :exclude [merge])
  (:require [pdok.cache :refer :all]
            [pdok.postgres :as pg]
            [pdok.featured.projectors :as proj]
            [pdok.featured.persistence :as pers]
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
               [:valid_from "timestamp without time zone"]
               [:valid_to "timestamp without time zone"]
               [:feature "text"]))

(defn- create-current-table [db]
  (pg/create-table db *timeline-schema* *current-table*
               [:id "serial" :primary :key]
               [:dataset "varchar(100)"]
               [:collection "varchar(255)"]
               [:feature_id "varchar(100)"]
               [:valid_from "timestamp without time zone"]
               [:feature "text"])
  (pg/create-index db *timeline-schema* *current-table* :dataset :collection :feature_id))

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

(defn- init-root
  ([feature]
   (if-let [dataset (:dataset feature)]
     {:_dataset dataset :_collection (:collection feature) :_id (:id feature)}
     {:_collection (:collection feature) :_id (:id feature)}))
  ([collection id]
   {:_collection collection :_id id})
  ([dataset collection id]
   {:_dataset dataset :_collection collection :_id id}))

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

(comment (merge {:a 1 :b [{:_id 1 :c 2} {:_id 2 :c 3}]} [["a" 1] [:G 2]] {:dataset "DD" :collection "test" :id 12 :attributes {"ZZ" 12}}))

(defn- merge
  ([target path feature]
   (let [keyworded-path (map (fn [[_ id field]] [(keyword field) id]) path)
         mustafied (mustafy feature)
         merger (path->merge-fn target keyworded-path)
         merged (merger mustafied)]
     merged)))

(defn- sync-valid-from [acc feature]
  (assoc acc :_valid_from  (:validity feature)))

(defn- sync-valid-to [acc feature]
  (assoc acc :_valid_to (:validity feature)))

(defn- new-current-sql []
  (str "INSERT INTO " (qualified-current) " (dataset, collection, feature_id, valid_from, feature)
VALUES (?, ?, ? ,?, ?)"))

(defn- new-current
  ([db features]
   (try
     (let [transform-fn (juxt :_dataset :_collection :_id :_valid_from pg/to-json)
           records (map transform-fn features)]
       (j/execute! db (cons (new-current-sql) records) :multi? true :transaction? false))
      (catch java.sql.SQLException e (j/print-sql-exception-chain e))))
  )

(defn- get-current [db dataset collection id]
  (j/with-db-connection [c db]
    (let [results
          (j/query c [(str "SELECT  FROM " (qualified-current)
                           " WHERE dataset = ? AND collection = ?  AND feature_id = ?")
                      dataset collection id])]
      (pg/from-json (:feature (first results))))))

(defn- load-current-feature-cache [db dataset collection]
  (j/with-db-connection [c db]
    (let [results
          (j/query c [(str "SELECT dataset, collection,  feature_id, feature FROM " (qualified-current)
                           " WHERE dataset = ? AND collection = ?")
                      dataset collection] :as-arrays? true)
          for-cache
          (map #(vector (take 3 %1) (pg/from-json (last %1)) ) (drop 1 results))]
      for-cache)))

(defn- update-current-sql []
  (str "UPDATE " (qualified-current) "
SET feature = ?,
    valid_from = ?
WHERE dataset = ? AND collection = ? AND  feature_id = ?"))

(defn- update-current [db features]
  (try
    (let [transform-fn (juxt pg/to-json :_valid_from :_dataset :_collection :_id)
          records (map transform-fn features)]
      (j/execute! db (cons (update-current-sql) records) :multi? true :transaction? false))
    (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(defn- new-history-sql []
  (str "INSERT INTO " (qualified-history) " (dataset, collection, feature_id, valid_from, valid_to, feature)
VALUES (?, ?, ?, ?, ?, ?)"))

(defn- new-history [db features]
  (try
    (let [transform-fn (juxt :_dataset :_collection :_id :_valid_from :_valid_to pg/to-json)
          records (map transform-fn features)]
      (j/execute! db (cons (new-history-sql) records) :multi? true :transaction? false))
    (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(defn- feature-key [feature]
  [(:dataset feature) (:collection feature) (:id feature)])

;; TODO batches moeten gelinked zijn, want update moet na new. Als update eerder flushed gaat het mis
(deftype Timeline [db root-fn path-fn
                   feature-cache load-cache?-fn
                   new-current-batch new-current-batch-size
                   update-current-batch update-current-batch-size
                   new-history-batch new-history-batch-size]
  proj/Projector
  (init [this]
    (init db) this)
  (proj/new-feature [_ feature]
    (let [[dataset collection id] (feature-key feature)
          [root-col root-id] (root-fn dataset collection id)
          path (path-fn dataset collection id)
          cache-store-key-fn (fn [f] [(:_dataset f) (:_collection f) (:_id f)])
          cache-value-fn (fn [_ f] f)
          cache-use-key-fn (fn [dataset collection id] [dataset collection id])
          batched-new (with-batch new-current-batch new-current-batch-size (partial new-current db))
          cache-batched-new (with-cache feature-cache batched-new cache-store-key-fn cache-value-fn)
          batched-update (with-batch update-current-batch update-current-batch-size (partial update-current db))
          cache-batched-update (with-cache feature-cache batched-update cache-store-key-fn cache-value-fn)
          batched-history (with-batch new-history-batch new-history-batch-size (partial new-history db))
          load-cache (fn [dataset collection id]
                       (when (load-cache?-fn dataset collection)
                         (load-current-feature-cache db dataset collection)))
          cached-get-current (use-cache feature-cache cache-use-key-fn load-cache)]
      (if-let [current (cached-get-current dataset root-col root-id)]
        (if (t/before? (:_valid_from current) (:validity feature))
          (do ;(println "NOT-SAME")
              (batched-history (sync-valid-to current feature))
              (cache-batched-update (sync-valid-from (merge current path feature) feature)))
          (do ;(println "SAME")
              (cache-batched-update (sync-valid-from (merge current path feature) feature))))
        (cache-batched-new (sync-valid-from (merge (init-root dataset root-col root-id) path feature) feature)))))
  (proj/change-feature [_ feature]
    ;; change can be the same, because a new nested feature validity change will also result in a new validity
    ;(println "CHANGE")
    (proj/new-feature _ feature))
  (proj/close-feature [_ feature]
    (proj/new-feature _ feature))
  (proj/close [this]
    (flush-batch new-current-batch (partial new-current db))
    (flush-batch update-current-batch (partial update-current db))
    (flush-batch new-history-batch (partial new-history db))
    this)
  )

(defn create [config]
  (let [db (:db-config config)
        persistence (or (:persistence config) (pers/cached-jdbc-processor-persistence config))
        cache (ref (cache/basic-cache-factory {}))
        load-cache?-fn (once-true-fn)
        new-current-batch-size (or (:new-current-batch-size config) (:batch-size config) 10000)
        new-current-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        update-current-batch-size (or (:update-current-batch-size config) (:batch-size config) 10000)
        update-current-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        new-history-batch-size (or (:new-history-batch-size config) (:batch-size config) 10000)
        new-history-batch (ref (clojure.lang.PersistentQueue/EMPTY))]
    (->Timeline db (partial pers/root persistence) (partial pers/path persistence)
                cache load-cache?-fn
                new-current-batch new-current-batch-size
                update-current-batch update-current-batch-size
                new-history-batch new-history-batch-size)))
