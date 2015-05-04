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

(defn- all-fields-constructor [attributes]
  (if (empty? attributes) (constantly nil) (apply juxt (map #(fn [col] (get col %)) attributes))))

(defn- gs-add-feature
  ([db all-attributes-fn features]
   (try
     (let [per-dataset-collection
           (group-by #(select-keys % [:dataset :collection]) features)
           ]
       (doseq [[{:keys [dataset collection]} grouped-features] per-dataset-collection]
         (j/with-db-connection [c db]
           (let [all-attributes (all-attributes-fn dataset collection)
                 records (map #(feature-to-sparse-record % (all-fields-constructor all-attributes)) grouped-features)
                 fields (concat [:_id :_geometry] (map (comp keyword pg/quoted) all-attributes))]
                          (apply (partial j/insert! c (str dataset "." (pg/quoted collection)) fields) records)))))
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
  (init [this] this)
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
  (close [this]
    (let [cached-collection-attributes (cached cache gs-collection-attributes db)]
      (flush-batch insert-batch (partial gs-add-feature db cached-collection-attributes))
      (flush-batch update-batch (partial gs-update-feature db))
      (flush-batch delete-batch (partial gs-delete-feature db)))
    this))


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
