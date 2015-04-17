(ns pdok.featured.projectors
  (:require [pdok.cache :refer :all]
            [pdok.featured.feature :refer [as-jts]]
            [pdok.postgres :as pg]
            [clojure.core.cache :as cache]
            [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [environ.core :refer [env]]))

(defprotocol Projector
  (init [_])
  (new-feature [proj feature])
  (change-feature [proj feature])
  (close-feature [proj feature])
  (close [proj]))

(defn- remove-keys [map keys]
  (apply dissoc map keys))

(defn- conj!-when
  ([target delegate src & srcs]
    (let [nw (if (delegate src) (conj! target src) target)]
      (if (empty? srcs)
        nw
        (recur nw delegate (first srcs) (rest srcs))))))

(defn- conj!-when-not-nil [target src & srcs]
  (apply conj!-when target identity src srcs))

(defn- gs-dataset-exists? [db dataset]
  ;(println "dataset exists?")
  (pg/schema-exists? db dataset))

(defn- gs-create-dataset [db dataset]
  (pg/create-schema db dataset))

(defn- gs-collection-exists? [db dataset collection]
  ;(println "collection exists?")
  (pg/table-exists? db dataset collection))

(defn- gs-create-collection [db dataset collection]
  "Create table with default fields"
  (pg/create-table db dataset collection
                [:gid "serial" :primary :key]
                [:_id "varchar(100)"]
                [:_geometry "geometry"])
  (pg/create-index db dataset collection "_id"))

(defn- gs-collection-attributes [db dataset collection]
  ;(println "attributes")
  (let [columns (pg/table-columns db dataset collection)
        no-defaults (filter #(not (some #{(:column_name %)} ["gid" "_id" "_geometry"])) columns)
        attributes (map #(:column_name %) no-defaults)]
    attributes))

(defn- gs-add-attribute [db dataset collection attribute-name attribute-type]
  (try
    (pg/add-column db dataset collection attribute-name attribute-type)
    (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(defn- feature-to-sparse-record [{:keys [id geometry attributes]} all-fields-constructor]
  (let [sparse-attributes (all-fields-constructor attributes)
        record (concat [id (-> geometry as-jts)] sparse-attributes)]
    record))

(defn- feature-keys [feature]
  (let [geometry? (contains? feature :geometry)]
    (-> (apply conj!-when-not-nil
               (transient [])
               (when geometry? :_geometry)
               (keys (:attributes feature)))
        (persistent!))))

(defn- feature-to-update-record [{:keys [id geometry attributes]}]
  (let [attr-vals (vals attributes)
        rec (conj!-when-not-nil (transient []) (as-jts geometry))
        rec (apply conj!-when rec (fn [_] true) attr-vals)
        rec (conj! rec id)]
    (persistent! rec)))

(defn- gs-add-feature
  ([db all-attributes-fn features]
   (try
     (let [per-dataset-collection
           (group-by #(select-keys % [:dataset :collection]) features)
           ]
       (doseq [[{:keys [dataset collection]} grouped-features] per-dataset-collection]
         (j/with-db-connection [c db]
           (let [all-attributes (all-attributes-fn dataset collection)
                 all-fields-constructor (apply juxt (map #(fn [col] (get col %)) all-attributes))
                 records (map #(feature-to-sparse-record % all-fields-constructor) grouped-features)
                 fields (concat [:_id :_geometry] (map keyword all-attributes))]
             (apply
              (partial j/insert! c (str dataset "." collection) fields) records)))))
      (catch java.sql.SQLException e (j/print-sql-exception-chain e))))
  )

(defn- gs-update-sql [schema table columns]
  (str "UPDATE " (pg/quoted schema) "." (pg/quoted table)
       " SET " (str/join "," (map #(str (pg/quoted %) " = ?") columns))
       " WHERE \"_id\" = ?;"))

(defn- gs-update-feature [db features]
  (try
    (let [per-dataset-collection
          (group-by #(select-keys % [:dataset :collection]) features)]
      (doseq [[{:keys [dataset collection]} collection-features] per-dataset-collection]
        ;; group per key collection so we can batch every group
        (let [keyed (group-by feature-keys collection-features)]
          (doseq [[columns vals] keyed]
            (when (< 0 (count columns))
              (let [sql (gs-update-sql dataset collection (map name columns))
                    update-vals (map feature-to-update-record vals)]
                (j/execute! db (cons sql update-vals) :multi? true :transaction? false)))))
        ))
    ;; (catch java.sql.SQLException e (j/print-sql-exception-chain e))
    ))

(defn- gs-delete-sql [schema table]
  (str "DELETE FROM " (pg/quoted schema) "." (pg/quoted table)
       " WHERE \"_id\" = ?"))

(defn- gs-delete-feature [db features]
  (try
    (let [per-dataset-collection
          (group-by #(select-keys % [:dataset :collection]) features)]
      (doseq [[{:keys [dataset collection]} collection-features] per-dataset-collection]
        (let [sql (gs-delete-sql dataset collection)
              ids (map #(vector (:id %)) collection-features)]
          (j/execute! db (cons sql ids) :multi? true :transaction? false))))))

(deftype GeoserverProjector [db cache insert-batch insert-batch-size
                             update-batch update-batch-size delete-batch delete-batch-size]
  Projector
  (init [_])
  (new-feature [_ feature]
    (let [{:keys [dataset collection attributes]} feature
          cached-dataset-exists? (cached cache gs-dataset-exists? db)
          cached-collection-exists? (cached cache gs-collection-exists? db)
          cached-collection-attributes (cached cache gs-collection-attributes db)
          batched-add-feature
          (with-batch insert-batch insert-batch-size (partial gs-add-feature db cached-collection-attributes))]
      (do (when (not (cached-dataset-exists? dataset))
            (gs-create-dataset db dataset)
            (cached-dataset-exists? :reload dataset))
          (when (not (cached-collection-exists? dataset collection))
            (gs-create-collection db dataset collection)
            (cached-collection-exists? :reload dataset collection))
          (let [current-attributes (cached-collection-attributes dataset collection)
                new-attributes (filter #(not (some #{(first %)} current-attributes)) attributes)]
            (doseq [a new-attributes]
              (gs-add-attribute db dataset collection (first a) (-> a second type)))
            (when (not-empty new-attributes) (cached-collection-attributes :reload dataset collection)))
          (batched-add-feature feature))))
  (change-feature [_ feature]
    (let [{:keys [dataset collection attributes]} feature
          cached-collection-attributes (cached cache gs-collection-attributes db)
          batched-update-feature (with-batch update-batch update-batch-size
                                   (partial gs-update-feature db))]
      (let [current-attributes (cached-collection-attributes dataset collection)
            new-attributes (filter #(not (some #{(first %)} current-attributes)) attributes)]
        (doseq [a new-attributes]
          (gs-add-attribute db dataset collection (first a) (-> a second type)))
        (when (not-empty new-attributes) (cached-collection-attributes :reload dataset collection)))
      (batched-update-feature feature)))
  (close-feature [_ feature]
    (let [{:keys [dataset collection]} feature
          batched-delete-feature (with-batch delete-batch delete-batch-size
                                   (partial gs-delete-feature db))]
      (batched-delete-feature feature)))
  (close [_]
    (let [cached-collection-attributes (cached cache gs-collection-attributes db)]
      (flush-batch insert-batch (partial gs-add-feature db cached-collection-attributes))
      (flush-batch update-batch (partial gs-update-feature db))
      (flush-batch delete-batch (partial gs-delete-feature db)))))

(comment Een simpele tabel met een alle relevante (parent + childs) events in een array. Een trigger zorgt ervoor dat de huidige wordt dichtgezet en naar een history tabel wordt gezet.)

(def ^:dynamic *pc-schema* "parent_child")
(def ^:dynamic *history-table* "history")
(def ^:dynamic *current-table* "current")

(defn- pc-create-history-table [db]
  "Create table with default fields"
  (pg/create-table db *pc-schema* *history-table*
               [:id "serial" :primary :key]
               [:dataset "varchar(100)"]
               [:collection "varchar(255)"]
               [:feature_id "varchar(100)"]
               [:valid_from "timestamp without time zone"]
               [:valid_to "timestamp without time zone"]
               [:stream "text[]"]))

(defn- pc-create-current-table [db]
  (pg/create-table db *pc-schema* *current-table*
               [:id "serial" :primary :key]
               [:dataset "varchar(100)"]
               [:collection "varchar(255)"]
               [:feature_id "varchar(100)"]
               [:valid_from "timestamp without time zone"]
               [:stream "text[]"])
  (pg/create-index db *pc-schema* *current-table* :dataset :collection :feature_id))

(defn- pc-init [db]
  (when-not (pg/schema-exists? db *pc-schema*)
    (pg/create-schema db *pc-schema*))
  (when-not (pg/table-exists? db *pc-schema* *history-table*)
    (pc-create-history-table db))
  (when-not (pg/table-exists? db *pc-schema* *current-table*)
    (pc-create-current-table db)))

(defn- qualified-history []
  (str (name *pc-schema*) "." (name *history-table*)))

(defn- qualified-current []
  (str (name *pc-schema*) "." (name *current-table*)))

(defn- pc-collection [feature]
  (or (:parent-collection feature) (:collection feature)))

(defn- pc-id [feature]
  (or (:parent-id feature) (:id feature)))

(defn- pc-new-stream-sql []
  (str "INSERT INTO " (qualified-current) " (dataset, collection, feature_id, valid_from, stream)
VALUES (?, ?, ? ,?, ARRAY[?])"))

(defn- pc-new-stream
  ([db features]
   (try
     (let [transform-fn (juxt :dataset pc-collection pc-id :validity identity)
           records (map transform-fn features)]
       (j/execute! db (cons (pc-new-stream-sql) records) :multi? true :transaction? false))
      (catch java.sql.SQLException e (j/print-sql-exception-chain e))))
  )

(defn- pc-stream-exists? [db dataset collection id]
  (j/with-db-connection [c db]
    (let [results
          (j/query c [(str "SELECT id FROM " (qualified-current)
                           " WHERE dataset = ? AND collection = ?  AND feature_id = ?")
                      dataset collection id])]
      (not (empty? results)))))

(defn- pc-load-stream-cache [db dataset collection]
  (j/with-db-connection [c db]
    (let [results
          (j/query c [(str "SELECT dataset, collection  feature_id FROM " (qualified-current)
                           " WHERE dataset = ? AND collection = ?")
                      dataset collection] :as-arrays? true)]
      (map #(vector % true) results))))

(defn- pc-append-to-stream-sql []
  (str "UPDATE " (qualified-current) "
SET stream = array_append(stream, CAST (? as TEXT)),
    valid_from = ?
WHERE dataset = ? AND collection = ? AND  feature_id = ?"))

(defn- pc-append-to-stream [db features]
  (try
    (let [transform-fn (juxt identity :validity :dataset pc-collection pc-id)
          records (map transform-fn features)]
      (j/execute! db (cons (pc-append-to-stream-sql) records) :multi? true :transaction? false))
     (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(deftype ParentChildProjector [db stream-cache load-cache?-fn insert-batch insert-batch-size
                               update-batch update-batch-size]
  Projector
  (init [_]
    (pc-init db))
  (new-feature [_ feature]
    (let [cache-add-key-fn (fn [f] [(:dataset f) (pc-collection f) (pc-id f)])
          cache-add-value-fn (fn [_ f] true)
          cache-use-key-fn (fn [dataset collection id] [dataset collection id])
          batched-new (with-batch insert-batch insert-batch-size (partial pc-new-stream db))
          cache-batched-new (with-cache stream-cache batched-new cache-add-key-fn cache-add-value-fn)
          batched-append (with-batch update-batch update-batch-size (partial pc-append-to-stream db))
          load-cache (fn [dataset collection id]
                       (when (load-cache?-fn dataset collection)
                         (pc-load-stream-cache db dataset collection)))
          cached-stream-exists? (use-cache stream-cache cache-use-key-fn load-cache)]
      (if (cached-stream-exists? (:dataset feature) (pc-collection feature) (pc-id feature))
        (batched-append feature)
        (cache-batched-new feature))))
  (change-feature [_ feature]
    (new-feature _ feature))
  (close-feature [_ feature]
    (new-feature _ feature))
  (close [_]
    (flush-batch insert-batch (partial pc-new-stream db))
    (flush-batch update-batch (partial pc-append-to-stream db)))
  )

(defn geoserver-projector [config]
  (let [db (:db-config config)
        cache (atom {})
        insert-batch-size (or (:insert-batch-size config) (:batch-size config) 10000)
        insert-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        update-batch-size (or (:update-batch-size config) (:batch-size config) 10000)
        update-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        delete-batch-size (or (:delete-batch-size config) (:batch-size config) 10000)
        delete-batch (ref (clojure.lang.PersistentQueue/EMPTY))]
    (->GeoserverProjector db cache insert-batch insert-batch-size
                         update-batch update-batch-size delete-batch delete-batch-size)))

(defn once-true-fn
  "Returns a function which returns true once for an argument"
  []
  (let [mem (atom {})]
    (fn [& args]
      (if-let [e (find @mem args)]
        false
        (do
          (swap! mem assoc args nil)
          true)))))

(defn parent-child-projector [config]
  (let [db (:db-config config)
        cache (ref (cache/basic-cache-factory {}))
        load-cache?-fn (once-true-fn)
        insert-batch-size (or (:insert-batch-size config) (:batch-size config) 10000)
        insert-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        update-batch-size (or (:update-batch-size config) (:batch-size config) 10000)
        update-batch (ref (clojure.lang.PersistentQueue/EMPTY))]
    (->ParentChildProjector db cache load-cache?-fn insert-batch insert-batch-size update-batch update-batch-size)))
