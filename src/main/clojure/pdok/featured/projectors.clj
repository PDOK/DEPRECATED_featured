(ns pdok.featured.projectors
  (:require [pdok.cache :refer :all]
            [pdok.featured.feature :as f :refer [as-jts]]
            [pdok.postgres :as pg]
            [clojure.core.cache :as cache]
            [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))

(defprotocol Projector
  (init [proj])
  (new-feature [proj feature])
  (change-feature [proj feature])
  (close-feature [proj feature])
  (delete-feature [proj feature])
  (close [proj]))

(defn- remove-keys [map keys]
  (apply dissoc map keys))

(defn conj!-coll [target x & xs]
  (if (or (seq? x) (vector? x))
    (apply conj!-coll target x)
    (if (empty? xs)
      (conj! target x)
      (recur (conj! target x) (first xs) (rest xs)))))

(def visualization-table
  {:bgt {:wegdeel$kruinlijn "kruinlijn"}})

(defn visualization [dataset collection]
  (let [k-collection (keyword collection)
        k-dataset (keyword dataset)]
    (or (get-in visualization-table [k-dataset k-collection] collection))))

(defn- gs-dataset-exists? [db dataset]
  ;(println "dataset exists?")
  (pg/schema-exists? db dataset))

(defn- gs-create-dataset [db dataset]
  (pg/create-schema db dataset))

(defn- gs-collection-exists? [db dataset collection]
  (let [table (visualization dataset collection)]
    (pg/table-exists? db dataset table)))

(defn- gs-create-collection [db ndims srid dataset collection]
  "Create table with default fields"
  (let [table (visualization dataset collection)]

      (pg/create-table db dataset table
                [:gid "serial" :primary :key]
                [:_id "varchar(100)"]
                [:_version "uuid"]
                [:_geometry_point "geometry"]
                [:_geometry_line "geometry"]
                [:_geometry_polygon "geometry"]
                [:_geo_group "varchar (20)"])

      (pg/create-index db dataset table "_id")
      (pg/add-geo-constraints db dataset table :_geometry_point ndims srid)
      (pg/add-geo-constraints db dataset table :_geometry_line ndims srid)
      (pg/add-geo-constraints db dataset table :_geometry_polygon ndims srid)
      (pg/populate-geometry-columns db dataset table)))

(defn- gs-collection-attributes [db dataset collection]
  ;(println "attributes")
  (let [table (visualization dataset collection)
        columns (pg/table-columns db dataset table)
        no-defaults (filter #(not (some #{(:column_name %)} ["gid"
                                                             "_id"
                                                             "_version"
                                                             "_geometry_point"
                                                             "_geometry_line"
                                                             "_geometry_polygon"
                                                             "_geo_group"])) columns)
        attributes (map #(:column_name %) no-defaults)]
    attributes))

(defn- gs-add-attribute [db dataset collection attribute-name attribute-type]
  (let [table (visualization dataset collection)]
    (try
      (pg/add-column db dataset table attribute-name attribute-type)
      (catch java.sql.SQLException e
        (log/with-logs ['pdok.featured.projectors :error :error] (j/print-sql-exception-chain e))))))

(defn- feature-to-sparse-record [proj-fn feature all-fields-constructor]
  (let [id (:id feature)
        version (:version feature)
        sparse-attributes (all-fields-constructor (:attributes feature))]
    (when-let [geometry (proj-fn (-> feature (:geometry) (f/as-jts)))]
      (let [geo-group (f/geometry-group (:geometry feature))
            record (concat [id
                            version
                            (when (= :point geo-group)  geometry)
                            (when (= :line geo-group)  geometry)
                            (when (= :polygon geo-group)  geometry)
                            geo-group]
                           sparse-attributes)]
        record))))

(defn- feature-keys [feature]
  (let [geometry (:geometry feature)
        attributes (:attributes feature)]
     (cond-> (transient [])
             geometry (conj!-coll :_geometry_point :_geometry_line :_geometry_polygon :_geo_group)
             attributes (conj!-coll (keys attributes))
             true (persistent!))))

 (defn- feature-to-update-record [proj-fn feature]
   (let [attributes (:attributes feature)
         geometry (:geometry feature)
         geo-group (when geometry (f/geometry-group geometry))]
     (cond-> (transient [(:version feature)])
             geometry (conj!-coll
                       (when (= :point geo-group) (proj-fn (f/as-jts geometry)))
                       (when (= :line geo-group) (proj-fn (f/as-jts geometry)))
                       (when (= :polygon geo-group)  (proj-fn (f/as-jts geometry)))
                       geo-group)
             attributes (conj!-coll (vals attributes))
             true (conj! (:id feature))
             true (conj! (:current-version feature))
             true (persistent!))))

(defn- all-fields-constructor [attributes]
  (if (empty? attributes) (constantly nil) (apply juxt (map #(fn [col] (get col %)) attributes))))

(defn- geo-column [geo-group]
   (str "_geometry_" (name geo-group)))

(defn- gs-add-feature
  ([db proj-fn all-attributes-fn features]
   (try
     (let [selector (juxt :dataset :collection)
           per-dataset-collection (group-by selector features)]
        (doseq [[[dataset collection] grouped-features] per-dataset-collection]
         (j/with-db-connection [c db]
           (let [all-attributes (all-attributes-fn dataset collection)
                 records (filter (comp not nil?)
                                 (map #(feature-to-sparse-record proj-fn % (all-fields-constructor all-attributes)) grouped-features))]
              (if (not (empty? records))
                (let [fields (concat [:_id :_version :_geometry_point :_geometry_line :_geometry_polygon :_geo_group]
                                   (map pg/quoted all-attributes))]
                  (apply (partial j/insert! c (str dataset "." (pg/quoted (visualization dataset collection))) fields)
                       records)))))))
     (catch java.sql.SQLException e
       (log/with-logs ['pdok.featured.projectors :error :error] (j/print-sql-exception-chain e))))))


(defn- gs-update-sql [schema table columns]
  (str "UPDATE " (pg/quoted schema) "." (pg/quoted table)
       " SET \"_version\" = ?, " (str/join "," (map #(str (pg/quoted %) " = ?") columns))
       " WHERE \"_id\" = ? and \"_version\" = ?;"))

(defn- execute-update-sql [db dataset collection columns update-vals]
  (try
    (let [sql (gs-update-sql dataset (visualization dataset collection) (map name columns))]
      (j/execute! db (cons sql update-vals) :multi? true :transaction? false))
    (catch java.sql.SQLException e
      (log/with-logs ['pdok.featured.projectors :error :error] (j/print-sql-exception-chain e)))))

(defn- gs-update-feature [db proj-fn features]
    (let [selector (juxt :dataset :collection)
         per-dataset-collection
           (group-by selector features)]
     (doseq [[[dataset collection] collection-features] per-dataset-collection]
        ;; group per key collection so we can batch every group
        (let [keyed (group-by feature-keys collection-features)]
          (doseq [[columns vals] keyed]
            (when (< 0 (count columns))
               (let [update-vals (map feature-to-update-record proj-fn vals)]
                 (execute-update-sql db dataset collection columns update-vals)
               )))))))


(defn- gs-delete-sql [schema table]
  (str "DELETE FROM " (pg/quoted schema) "." (pg/quoted table)
       " WHERE \"_id\" = ? and \"_version\" = ?"))

(defn- gs-delete-feature [db features]
  (try
    (let [selector (juxt :dataset :collection)
          per-dataset-collection (group-by selector features)]
       (doseq [[[dataset collection] collection-features] per-dataset-collection]
        (let [sql (gs-delete-sql dataset (visualization dataset collection))
              ids (map #(vector (:id %1) (:current-version %1)) collection-features)]
          (j/execute! db (cons sql ids) :multi? true :transaction? false))))
    (catch java.sql.SQLException e
      (log/with-logs ['pdok.featured.projectors :error :error] (j/print-sql-exception-chain e)))))

(defn- flush-all [db proj-fn cache insert-batch update-batch delete-batch]
  "Used for flushing all batches, so entry order is alway new change close"
  (let [cached-collection-attributes (cached cache gs-collection-attributes db)]
    (flush-batch insert-batch (partial gs-add-feature db proj-fn cached-collection-attributes))
    (flush-batch update-batch (partial gs-update-feature db proj-fn))
    (flush-batch delete-batch (partial gs-delete-feature db))))

(deftype GeoserverProjector [db cache insert-batch insert-batch-size
                             update-batch update-batch-size
                             delete-batch delete-batch-size
                             flush-fn proj-fn ndims srid]
  Projector
  (init [this] this)
  (new-feature [_ feature]
    (let [{:keys [dataset collection attributes]} feature
          cached-dataset-exists? (cached cache gs-dataset-exists? db)
          cached-collection-exists? (cached cache gs-collection-exists? db)
          cached-collection-attributes (cached cache gs-collection-attributes db)
          batched-add-feature
          (with-batch insert-batch insert-batch-size (partial gs-add-feature db proj-fn cached-collection-attributes) flush-fn)]
      (do (when (not (cached-dataset-exists? dataset))
            (gs-create-dataset db dataset)
            (cached-dataset-exists? :reload dataset))
          (when (not (cached-collection-exists? dataset collection))
            (gs-create-collection db ndims srid dataset collection)
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
                                   (partial gs-update-feature db proj-fn) flush-fn)]
      (let [current-attributes (cached-collection-attributes dataset collection)
            new-attributes (filter #(not (some #{(first %)} current-attributes)) attributes)]
        (doseq [a new-attributes]
          (gs-add-attribute db dataset collection (first a) (-> a second type)))
        (when (not-empty new-attributes) (cached-collection-attributes :reload dataset collection)))
      (batched-update-feature feature)))
  (close-feature [_ feature]
    (let [batched-delete-feature (with-batch delete-batch delete-batch-size
                                   (partial gs-delete-feature db) flush-fn)]
      (batched-delete-feature feature)))
  (delete-feature [_ feature]
    (let [batched-delete-feature (with-batch delete-batch delete-batch-size
                                   (partial gs-delete-feature db) flush-fn)]
      (batched-delete-feature feature)))
  (close [this]
    (flush-fn)
    this))


(defn geoserver-projector [config]
  (let [db (:db-config config)
        cache (atom {})
        insert-batch-size (or (:insert-batch-size config) (:batch-size config) 10000)
        insert-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        update-batch-size (or (:update-batch-size config) (:batch-size config) 10000)
        update-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        delete-batch-size (or (:delete-batch-size config) (:batch-size config) 10000)
        delete-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        ndims (or (:ndims config) 2)
        srid (or (:srid config) 28992)
        proj-fn (or (:proj-fn config) identity)
        flush-fn #(flush-all db proj-fn cache insert-batch update-batch delete-batch)]
    (->GeoserverProjector db cache insert-batch insert-batch-size
                          update-batch update-batch-size
                          delete-batch delete-batch-size
                          flush-fn proj-fn ndims srid)))
