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

(def point-types
  "Geometry types of Point-category"
  #{:Point :MultiPoint})

(def line-types
  "Geometry types of Line-category"
  #{:Line :LineString :MultiLine})

(defn- geometry-group*
  "Returns the geometry-group of the given type"
  [type]
  (condp get (keyword type)
    point-types :point
    line-types :line
    :polygon))

(defn- geometry-group-fn [geometry]
  (-> geometry .getGeometryType geometry-group*))

(defn- add-geometry-group [record]
  (let [[id geometry & more] record]
    {:geotype (geometry-group-fn geometry)}))

(defn- update-geometry-group [geometry-index update-val]
  (let [geometry (get update-val geometry-index)]
    {:geotype (geometry-group-fn geometry)}))

(defn- gs-collection-exists? [db dataset collection]
  (let [table (visualization dataset collection)]
    (pg/table-exists? db dataset table)))

(defn- gs-create-collection [db dataset collection]
  "Create table with default fields"
  (let [table (visualization dataset collection)]
    
      (pg/create-table db dataset table
                [:gid "serial" :primary :key]
                [:_id "varchar(100)"]
                [:_geometry_point "geometry"]
                [:_geometry_line "geometry"]
                [:_geometry_polygon "geometry"]
                [:_geo_group "varchar (20)"])
      
      (pg/create-index db dataset table "_id")
      (pg/add-geo-constraints db dataset table :_geometry_point)
      (pg/add-geo-constraints db dataset table :_geometry_line)
      (pg/add-geo-constraints db dataset table :_geometry_polygon)
      (pg/populate-geometry-columns db dataset table)))

(defn- gs-collection-attributes [db dataset collection]
  ;(println "attributes")
  (let [table (visualization dataset collection)
        columns (pg/table-columns db dataset table)
        no-defaults (filter #(not (some #{(:column_name %)} ["gid" 
                                                             "_id" 
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
      (catch java.sql.SQLException e (j/print-sql-exception-chain e)))))

(defn- feature-to-sparse-record [{:keys [id geometry attributes]} all-fields-constructor]
  (let [sparse-attributes (all-fields-constructor attributes)
        geometry (-> geometry as-jts)
        record (concat [id geometry (geometry-group-fn geometry)] sparse-attributes)]
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

(defn- geo-column [geogroup]
   (str "_geometry_" (name geogroup)))

(defn- gs-add-feature
  ([db all-attributes-fn features]
   (try
     (let [per-dataset-collection (group-by #(select-keys % [:dataset :collection]) features)]
       (doseq [[{:keys [dataset collection]} grouped-features] per-dataset-collection]
         (j/with-db-connection [c db]
           (let [all-attributes (all-attributes-fn dataset collection)
                 records (map #(feature-to-sparse-record % (all-fields-constructor all-attributes)) grouped-features)
                 per-dataset-collection-geotype (group-by add-geometry-group records)]   
                   (doseq [[{:keys [geotype]} grouped-records] per-dataset-collection-geotype]
                     ;; group per geotype so we can batch every group
                     (let [fields (concat [:_id (geo-column geotype) :_geo_group] (map (comp keyword pg/quoted) all-attributes))]
                       (apply (partial j/insert! c (str dataset "." (pg/quoted (visualization dataset collection))) fields) grouped-records)))))))
      (catch java.sql.SQLException e (j/print-sql-exception-chain e)))))


(defn- gs-update-sql [schema table columns]
  (str "UPDATE " (pg/quoted schema) "." (pg/quoted table)
       " SET " (str/join "," (map #(str (pg/quoted %) " = ?") columns))
       " WHERE \"_id\" = ?;"))

(defn- execute-update-sql [db dataset collection columns update-vals]
  (try
    (let [sql (gs-update-sql dataset (visualization dataset collection) (map name columns))]
      (j/execute! db (cons sql update-vals) :multi? true :transaction? false))
      (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(defn- gs-update-feature [db features]
    (let [per-dataset-collection
          (group-by #(select-keys % [:dataset :collection]) features)]
      (doseq [[{:keys [dataset collection]} collection-features] per-dataset-collection]
        ;; group per key collection so we can batch every group
        (let [keyed (group-by feature-keys collection-features)]
          (doseq [[columns vals] keyed]
            (let [geometry-index (.indexOf columns :_geometry)]
            (when (< 0 (count columns))
              (let [update-vals (map feature-to-update-record vals)]
                (if (= geometry-index -1)
                  (execute-update-sql db dataset collection columns update-vals)
                  (let [per-geotype (group-by (partial update-geometry-group geometry-index) update-vals)]
                    (doseq [[{:keys [geotype]} grouped-vals] per-geotype]
                      (let [columns (assoc columns geometry-index (geo-column geotype))]
                        (execute-update-sql db dataset collection columns update-vals)))))))))))))


(defn- gs-delete-sql [schema table]
  (str "DELETE FROM " (pg/quoted schema) "." (pg/quoted table)
       " WHERE \"_id\" = ?"))

(defn- gs-delete-feature [db features]
  (try
    (let [per-dataset-collection
          (group-by #(select-keys % [:dataset :collection]) features)]
      (doseq [[{:keys [dataset collection]} collection-features] per-dataset-collection]
        (let [sql (gs-delete-sql dataset (visualization dataset collection))
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
