(ns pdok.featured.projectors
  (:require [pdok.featured.cache :refer :all]
            [pdok.featured.feature :refer [as-wkt]]
            [pdok.postgres :as pg]
            [clojure.java.jdbc :as j]
            [environ.core :refer [env]])
  (:import [org.postgis PGgeometry]))

(defprotocol Projector
  (new-feature [proj feature])
  (close [proj]))

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
                [:feature_id "varchar(100)"]
                [:geometry "geometry"]))

(defn- gs-collection-attributes [db dataset collection]
  ;(println "attributes")
  (let [columns (pg/table-columns db dataset collection)
        no-defaults (filter #(not (some #{(:column_name %)} ["gid" "feature_id" "geometry"])) columns)
        attributes (map #(:column_name %) no-defaults)]
    attributes))

(defn- gs-add-attribute [db dataset collection attribute-name attribute-type]
  (pg/add-column db dataset collection attribute-name attribute-type))

(defn- as-PGgeometry [wkt]
  (PGgeometry/geomFromString wkt))

(defn- feature-to-record [{:keys [dataset collection id geometry attributes]} all-fields-constructor]
  (let [sparse-attributes (all-fields-constructor attributes)
        record (concat [id (-> geometry as-wkt as-PGgeometry)]
                       (map #(-> % pg/convert-clj-to-pg) sparse-attributes))]
    record))

(defn- gs-add-feature
  ([db  feature]
   (println "single")
   (gs-add-feature db  (map #(-> % first keyword) (:attributes feature)) (list feature)))
  ([db all-attributes-fn features]
   ;{:keys [dataset collection id geometry attributes]}
   (try
     (let [per-dataset-collection
           (group-by #(select-keys % [:dataset :collection]) features)
           ]
       (doseq [[{:keys [dataset collection]} grouped-features] per-dataset-collection]
         (j/with-db-connection [c db]
           (let [all-attributes (all-attributes-fn dataset collection)
                 all-fields-constructor (apply juxt (map #(fn [col] (get col %)) all-attributes))
                 records (map #(feature-to-record % all-fields-constructor) grouped-features)
                 fields (concat [:feature_id :geometry] (map keyword all-attributes))]
             (apply
              (partial j/insert! c (str dataset "." collection) fields) records)))))
      (catch java.sql.SQLException e (j/print-sql-exception-chain e))))
  )

(deftype GeoserverProjector [db cache batch batch-size]
    Projector
    (new-feature [_ feature]
      (let [{:keys [dataset collection geometry attributes]} feature
            cached-dataset-exists? (cached cache gs-dataset-exists? db)
            cached-collection-exists? (cached cache gs-collection-exists? db)
            cached-collection-attributes (cached cache gs-collection-attributes db)
            batched-add-feature
            (with-batch batch batch-size (partial gs-add-feature db cached-collection-attributes))]
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
    (close [_]
      (let [cached-collection-attributes (cached cache gs-collection-attributes db)]
        (flush-batch batch (partial gs-add-feature db cached-collection-attributes)))))

(defn geoserver-projector [config]
  (let [db (:db-config config)
        cache (atom {})
        batch-size (or (:batch-size config) 10000)
        batch (ref (clojure.lang.PersistentQueue/EMPTY))]
    (GeoserverProjector. db cache batch batch-size)))

(def ^:private data-db {:subprotocol "postgresql"
                     :subname (or (env :projector-database-url) "//localhost:5432/pdok")
                     :user (or (env :projector-database-user) "postgres")
                     :password (or (env :projector-database-password) "postgres")})
