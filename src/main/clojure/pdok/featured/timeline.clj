(ns pdok.featured.timeline
  (:require [pdok.cache :refer :all]
            [pdok.postgres :as pg]
            [pdok.featured.projectors :as proj]
            [clojure.java.jdbc :as j]
            [clojure.core.cache :as cache]))

(comment Een simpele tabel met een alle relevante (parent + childs) events in een array. Een trigger zorgt ervoor dat de huidige wordt dichtgezet en naar een history tabel wordt gezet.)

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
               [:stream "text[]"]))

(defn- create-current-table [db]
  (pg/create-table db *timeline-schema* *current-table*
               [:id "serial" :primary :key]
               [:dataset "varchar(100)"]
               [:collection "varchar(255)"]
               [:feature_id "varchar(100)"]
               [:valid_from "timestamp without time zone"]
               [:stream "text[]"])
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

(defn- collection [feature]
  (or (:parent-collection feature) (:collection feature)))

(defn- id [feature]
  (or (:parent-id feature) (:id feature)))

(defn- new-stream-sql []
  (str "INSERT INTO " (qualified-current) " (dataset, collection, feature_id, valid_from, stream)
VALUES (?, ?, ? ,?, ARRAY[?])"))

(defn- new-stream
  ([db features]
   (try
     (let [transform-fn (juxt :dataset collection id :validity identity)
           records (map transform-fn features)]
       (j/execute! db (cons (new-stream-sql) records) :multi? true :transaction? false))
      (catch java.sql.SQLException e (j/print-sql-exception-chain e))))
  )

(defn- stream-exists? [db dataset collection id]
  (j/with-db-connection [c db]
    (let [results
          (j/query c [(str "SELECT id FROM " (qualified-current)
                           " WHERE dataset = ? AND collection = ?  AND feature_id = ?")
                      dataset collection id])]
      (not (empty? results)))))

(defn- load-stream-cache [db dataset collection]
  (j/with-db-connection [c db]
    (let [results
          (j/query c [(str "SELECT dataset, collection  feature_id FROM " (qualified-current)
                           " WHERE dataset = ? AND collection = ?")
                      dataset collection] :as-arrays? true)]
      (map #(vector % true) results))))

(defn- append-to-stream-sql []
  (str "UPDATE " (qualified-current) "
SET stream = array_append(stream, CAST (? as TEXT)),
    valid_from = ?
WHERE dataset = ? AND collection = ? AND  feature_id = ?"))

(defn- append-to-stream [db features]
  (try
    (let [transform-fn (juxt identity :validity :dataset collection id)
          records (map transform-fn features)]
      (j/execute! db (cons (append-to-stream-sql) records) :multi? true :transaction? false))
     (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(deftype Timeline [db stream-cache load-cache?-fn insert-batch insert-batch-size
                               update-batch update-batch-size]
  proj/Projector
  (init [this]
    (init db) this)
  (proj/new-feature [_ feature]
    (let [cache-add-key-fn (fn [f] [(:dataset f) (collection f) (id f)])
          cache-add-value-fn (fn [_ f] true)
          cache-use-key-fn (fn [dataset collection id] [dataset collection id])
          batched-new (with-batch insert-batch insert-batch-size (partial new-stream db))
          cache-batched-new (with-cache stream-cache batched-new cache-add-key-fn cache-add-value-fn)
          batched-append (with-batch update-batch update-batch-size (partial append-to-stream db))
          load-cache (fn [dataset collection id]
                       (when (load-cache?-fn dataset collection)
                         (load-stream-cache db dataset collection)))
          cached-stream-exists? (use-cache stream-cache cache-use-key-fn load-cache)]
      (if (cached-stream-exists? (:dataset feature) (collection feature) (id feature))
        (batched-append feature)
        (cache-batched-new feature))))
  (proj/change-feature [_ feature]
    (proj/new-feature _ feature))
  (proj/close-feature [_ feature]
    (proj/new-feature _ feature))
  (proj/close [this]
    (flush-batch insert-batch (partial new-stream db))
    (flush-batch update-batch (partial append-to-stream db))
    this)
  )

(defn create [config]
  (let [db (:db-config config)
        cache (ref (cache/basic-cache-factory {}))
        load-cache?-fn (once-true-fn)
        insert-batch-size (or (:insert-batch-size config) (:batch-size config) 10000)
        insert-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        update-batch-size (or (:update-batch-size config) (:batch-size config) 10000)
        update-batch (ref (clojure.lang.PersistentQueue/EMPTY))]
    (->Timeline db cache load-cache?-fn insert-batch insert-batch-size update-batch update-batch-size)))
