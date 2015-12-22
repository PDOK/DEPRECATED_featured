(ns pdok.featured.timeline
  (:refer-clojure :exclude [merge])
  (:require [pdok.cache :refer :all]
            [pdok.postgres :as pg]
            [pdok.util :refer [with-bench]]
            [pdok.featured.projectors :as proj]
            [pdok.featured.persistence :as pers]
            [pdok.featured.tiles :as tiles]
            [joplin.core :as joplin]
            [joplin.jdbc.database]
            [clojure.java.jdbc :as j]
            [clojure.core.cache :as cache]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clj-time.core :as t]
             [clojure.core.async :as a
             :refer [>!! close!]]))

(declare create)

(defn indices-of [f coll]
  (keep-indexed #(if (f %2) %1 nil) coll))

(defn index-of [f coll]
  (first (indices-of f coll)))

(def ^:dynamic *timeline-schema* "featured")
(def ^:dynamic *history-table* "timeline")
(def ^:dynamic *current-table* "timeline_current")

(defn- init [db]
  (when-not (pg/schema-exists? db *timeline-schema*)
    (pg/create-schema db *timeline-schema*))
  (let [jdb {:db (assoc db
                        :type :jdbc
                        :url (pg/dbspec->url db))
             :migrator "/pdok/featured/migrations/timeline"
             :migrations-table "featured.timeline_migrations"}]
    (joplin/migrate-db jdb)))

(defn- qualified-history []
  (str (name *timeline-schema*) "." (name *history-table*)))

(defn- qualified-current []
  (str (name *timeline-schema*) "." (name *current-table*)))

(defn map->where-clause [clauses]
  (str/join " AND " (map #(str (name (first %1)) " = '" (second %1) "'") clauses)))

(defn- execute-query [timeline query]
  (try (j/with-db-connection [c (:db timeline)]
         (let [results (j/query c [query] :as-arrays? true)
               results (map (fn [[f]] (pg/from-json f)) (drop 1 results))]
           results))
       (catch java.sql.SQLException e
          (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e)))))

(defn current
  ([timeline dataset collection] (current timeline {:dataset dataset :collection collection}))
  ([timeline selector]
   (let [query (str "SELECT feature FROM " (qualified-current) " "
                    "WHERE valid_to is null AND " (map->where-clause selector))]
     (execute-query timeline query))))

(defn closed
  ([timeline dataset collection] (closed timeline {:dataset dataset :collection collection}))
  ([timeline selector]
   (let [query (str "SELECT feature FROM " (qualified-current) " "
                    "WHERE valid_to is not null AND " (map->where-clause selector))]
     (execute-query timeline query))))

(defn history
  ([timeline dataset collection] (history timeline {:dataset dataset :collection collection}))
  ([timeline selector]
   (let [query (str "SELECT feature FROM " (qualified-history) " "
                    "WHERE " (map->where-clause selector))]
     (execute-query timeline query))))

(defn- feature-on-channel [rc feature]
  (>!! rc (pg/from-json (:feature feature))))

(defn- execute-query-result-on-channel [timeline query rc]
  (try (j/with-db-connection [c (:db timeline)]
         (let [statement (j/prepare-statement (doto (j/get-connection c) (.setAutoCommit false)) query :fetch-size 10000)]
           (j/query c [statement] :row-fn (partial feature-on-channel rc)) )
         (close! rc))
       (catch java.sql.SQLException e
          (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e)))))

(defn all
  ([timeline dataset collection result-channel] (all timeline {:dataset dataset :collection collection} result-channel))
  ([timeline selector result-channel]
   (let [clause (map->where-clause selector)
         query (str "SELECT feature FROM " (qualified-history) " "
                    "WHERE " clause
                    " UNION ALL "
                    "SELECT feature FROM " (qualified-current) " "
                    "WHERE " clause)]
     (execute-query-result-on-channel timeline query result-channel))))

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

(defn drop-nth [v n]
  (into [] (concat (subvec v 0 n) (subvec v (inc n) (count v)))))


;; we only check for close in an existing vector, so we can remove the entry
;; we should check everywhere, but it is very unlikely that such a case can exist, because the processor
;; does not allow a close before a new.
(defn- path->merge-fn [target path action]
  (if-not (empty? path)
    (loop [[[field id] & rest] path
           t target
           f identity]
      (if field
        (let [field-value (get t field)]
          (if field-value
            (cond
             (vector? field-value)
             (if-let [i (index-of #(= id (:_id %)) field-value)]
               (if (and (= :close action) (not (seq rest)))
                 (recur rest nil (fn [_] (f (update-in t [field] (fn [v] (drop-nth v i))))))
                 (recur rest (get field-value i) #(f (update-in t [field i] (fn [v] (clojure.core/merge v %))))))
               ;; not found -> append
               (if (= :close action)
                 (recur rest nil f)
                 (recur rest {} #(f (assoc-in t [field (count field-value)] %)))))
             ;; if it is not a vector we are probable replacing existing values.
             ;; Treat this as a non existing value, ie. override value;
             :else (do ;(println "override:" field (f {}))
                       (recur rest (init-root field id) #(f (assoc t field (vector %)))))
             )
            ;; no field-value => we are working with child objects, place it in a vector
            (recur rest nil #(f (assoc t field (vector %))))))
        ;; no path (f {})
        f
          ))
    #(clojure.core/merge target %))
  )

(defn- merge
  ([target path feature]
   (let [keyworded-path (map (fn [[_ _ field id]] [(keyword field) id]) path)
         mustafied (mustafy feature)
         merger (path->merge-fn target keyworded-path (:action feature))
         merged (merger mustafied)
         merged (assoc merged :_version (:version feature))
         merged (update-in merged [:_all_versions] (fnil conj '()) (:version feature))
         merged (update merged :_tiles #((fnil clojure.set/union #{}) % (tiles/nl (:geometry feature))))]
     merged)))

(defn- sync-valid-from [acc feature]
  (assoc acc :_valid_from  (:validity feature)))

(defn- sync-valid-to [acc feature]
  (assoc acc :_valid_to (:validity feature)))

(defn- reset-valid-to [acc]
  (assoc acc :_valid_to nil))

(defn- new-current-sql []
  (str "INSERT INTO " (qualified-current)
       " (dataset, collection, feature_id, version, valid_from, valid_to, feature, tiles)
VALUES (?, ?, ? ,?, ?, ?, ?, ?)"))

(defn- new-current
  ([db features]
   (try
     (let [transform-fn (juxt :_dataset :_collection :_id :_version
                              :_valid_from :_valid_to pg/to-json :_tiles)
           records (map transform-fn features)]
       (j/execute! db (cons (new-current-sql) records) :multi? true :transaction? false))
     (catch java.sql.SQLException e
       (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e)))))
  )

;; (defn- get-current [db dataset collection id]
;;   (j/with-db-connection [c db]
;;     (let [results
;;           (j/query c [(str "SELECT  FROM " (qualified-current)
;;                            " WHERE dataset = ? AND collection = ?  AND feature_id = ?")
;;                       dataset collection id])]
;;       (pg/from-json (:feature (first results))))))

(defn- load-current-feature-cache-sql [n]
  (str "SELECT dataset, collection, feature_id, feature  FROM " (qualified-current)
       " WHERE dataset = ? AND collection = ? AND feature_id in ("
       (clojure.string/join "," (repeat n "?")) ")")
  )

(defn- load-current-feature-cache [db dataset collection ids]
  (try (j/with-db-connection [c db]
         (let [results
               (j/query c (apply vector (load-current-feature-cache-sql (count ids))
                                 dataset collection ids) :as-arrays? true)
               for-cache
               (map (fn [[ds col fid f]] [[ds col fid] (pg/from-json f)] ) (drop 1 results))]
           for-cache))
       (catch java.sql.SQLException e
          (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e)))))

(defn- delete-current-sql []
  (str "DELETE FROM " (qualified-current)
       " WHERE version = ?"))

(defn- delete-current [db versions]
  (try
    (let [records (map #(vector %) versions)]
      (j/execute! db (cons (delete-current-sql) records) :multi? true :transaction? false))
     (catch java.sql.SQLException e
       (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e)))))

(defn- new-history-sql []
  (str "INSERT INTO " (qualified-history) " (dataset, collection, feature_id, version, valid_from, valid_to, feature, tiles)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)"))

(defn- new-history [db features]
  (try
    (let [transform-fn (juxt :_dataset :_collection :_id :_version :_valid_from :_valid_to pg/to-json :_tiles)
          records (map transform-fn features)]
      (j/execute! db (cons (new-history-sql) records) :multi? true :transaction? false))
     (catch java.sql.SQLException e
       (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e)))))

(defn- delete-history-sql []
  (str "DELETE FROM " (qualified-history)
       " WHERE ARRAY[version] <@ ?"))

(defn- delete-history [db features]
  (try
    (let [transform-fn (juxt #(set (:_all_versions %)))
          records (map transform-fn features)]
      (j/execute! db (cons (delete-history-sql) records) :multi? true :transaction? false))
     (catch java.sql.SQLException e
       (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e)))))

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

(defrecord Timeline [db root-fn path-fn
                   feature-cache
                   new-current-batch new-current-batch-size
                   delete-current-batch delete-current-batch-size
                   new-history-batch new-history-batch-size
                   delete-history-batch delete-history-batch-size
                   flush-fn]
  proj/Projector
  (proj/init [this]
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
          cached-get-current (use-cache feature-cache cache-use-key)]
      (if-let [current (cached-get-current dataset root-col root-id)]
        (let [new-current (merge current path feature)]
          (if (= (:action feature) :close)
            (cache-batched-update (sync-valid-to new-current feature))
            (if (t/before? (:_valid_from current) (:validity feature))
              (do ;(println "NOT-SAME")
                        (batched-history (sync-valid-to current feature))
                ;; reset valid-to for new-current.
                        (cache-batched-update (reset-valid-to (sync-valid-from new-current feature))))
              (do ;(println "SAME")
                        ;; reset valid-to because it might be closed because of nested features.
                        (cache-batched-update (reset-valid-to (sync-valid-from new-current feature)))))))
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
          cached-get-current (use-cache feature-cache cache-use-key)]
      (when-let [current (cached-get-current dataset root-col root-id)]
        (cache-batched-delete current))))
  (proj/accept? [_ feature] true)
  (proj/close [this]
    (flush-fn)
    this)
  )


;; (let [selector (juxt :dataset :collection)
;;            per-dataset-collection (group-by selector features)]
;;         (doseq [[[dataset collection] grouped-features] per-dataset-collection]

(defn create-cache [db chunk]
  (let [selector (juxt :dataset :collection)
        per-d-c (group-by selector chunk)
        cache (ref (cache/basic-cache-factory {}))]
    (doseq [[[dataset collection] features] per-d-c]
      (apply-to-cache cache
                      (load-current-feature-cache db dataset collection (map :id features))))
    cache))

(defn process-chunk* [config chunk]
  (let [cache (create-cache (:db-config config) chunk)
        timeline (create config cache)]
    (doseq [f chunk]
      (condp = (:action f)
        :new (proj/new-feature timeline f)
        :change (proj/change-feature timeline f)
        :close (proj/close-feature timeline f)
        :delete (proj/delete-feature timeline f)))
    (proj/close timeline)))

(defn process-chunk [config chunk]
  (with-bench t (log/debug "Processed chunk in" t "ms")
    (flush-batch chunk (partial process-chunk* config))))

(defrecord ChunkedTimeline [config db chunk process-fn]
  proj/Projector
  (proj/init [this]
    (init db) this)
  (proj/new-feature [_ feature] (process-fn feature))
  (proj/change-feature [_ feature] (process-fn feature))
  (proj/close-feature [_ feature] (process-fn feature))
  (proj/delete-feature [_ feature] (process-fn feature))
  (proj/close [this] (process-chunk config chunk)))

(defn create
  ([config] (create config (ref (cache/basic-cache-factory {}))))
  ([config cache]
   (let [db (:db-config config)
         persistence (or (:persistence config) (pers/cached-jdbc-processor-persistence config))
         new-current-batch-size (or (:new-current-batch-size config) (:batch-size config) 10000)
         new-current-batch (ref (clojure.lang.PersistentQueue/EMPTY))
         delete-current-batch-size (or (:delete-current-batch-size config) (:batch-size config) 10000)
         delete-current-batch (ref (clojure.lang.PersistentQueue/EMPTY))
         new-history-batch-size (or (:new-history-batch-size config) (:batch-size config) 10000)
         new-history-batch (ref (clojure.lang.PersistentQueue/EMPTY))
         delete-history-batch-size (or (:delete-history-batch-size config) (:batch-size config) 10000)
         delete-history-batch (ref (clojure.lang.PersistentQueue/EMPTY))
         flush-fn #(flush-all db new-current-batch delete-current-batch new-history-batch delete-history-batch)]
     (->Timeline db (partial pers/root persistence) (partial pers/path persistence)
                 cache
                 new-current-batch new-current-batch-size
                 delete-current-batch delete-current-batch-size
                 new-history-batch new-history-batch-size
                 delete-history-batch delete-history-batch-size
                 flush-fn))))


(defn create-chunked [config]
  (let [chunk-size (or (:chunk-size config) 10000)
        chunk (ref (clojure.lang.PersistentQueue/EMPTY))
        process-fn (with-batch chunk chunk-size #() #(process-chunk config chunk))
        db (:db-config config)]
    (->ChunkedTimeline config db chunk process-fn)))
