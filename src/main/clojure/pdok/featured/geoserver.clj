(ns pdok.featured.geoserver
  (:require [pdok.featured.projectors :as proj]
            [pdok.cache :refer :all]
            [pdok.util :refer [checked]]
            [pdok.featured.feature :as f :refer [as-jts]]
            [pdok.postgres :as pg]
            [clojure.core.cache :as cache]
            [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import (clojure.lang PersistentQueue)))

(defn- remove-keys [map keys]
  (apply dissoc map keys))

(defn conj!-coll [target x & xs]
  (if (or (seq? x) (vector? x))
    (apply conj!-coll target x)
    (if (empty? xs)
      (conj! target x)
      (recur (conj! target x) (first xs) (rest xs)))))


(defn do-visualization? [options]
  (if (some #{"visualization"} options) true))

(defn- gs-dataset-exists? [{:keys [db dataset]}]
  ;(println "dataset exists?")
  (pg/schema-exists? db dataset))

(defn- gs-create-dataset [{:keys [db dataset]}]
  (pg/create-schema db dataset))

(defn- gs-collection-exists? [{:keys [db dataset]} collection]
  (let [table collection]
    (pg/table-exists? db dataset table)))

(defn- gs-create-collection [{:keys [db dataset]} ndims srid collection]
  "Create table with default fields"
  (let [table collection]

      (pg/create-table db dataset table
                [:gid "serial" :primary :key]
                [:_id "text"]
                [:_version "uuid"]
                [:_geo_group "text"])

      (pg/create-index db dataset table "_id")
      (pg/create-geometry-columns db dataset table :_geometry_point)
      (pg/create-geometry-columns db dataset table :_geometry_line)
      (pg/create-geometry-columns db dataset table :_geometry_polygon)
      (pg/create-geo-index db dataset table :_geometry_point)
      (pg/create-geo-index db dataset table :_geometry_line)
      (pg/create-geo-index db dataset table :_geometry_polygon)))

(defn- gs-collection-attributes [{:keys [db dataset]} collection]
  ;(println "attributes")
  (let [table collection
        columns (pg/table-columns db dataset table)
        no-defaults (filter #(not (some #{(:column_name %)} ["gid"
                                                             "_id"
                                                             "_version"
                                                             "_geometry_point"
                                                             "_geometry_line"
                                                             "_geometry_polygon"
                                                             "_geo_group"])) columns)
        attributes (reduce (fn [acc c] (conj acc c)) #{} (map #(:column_name %) no-defaults))]
    attributes))

(defn- gs-add-attribute [{:keys [db dataset]} collection attribute-name attribute-value]
  (let [table collection]
    (try
      (pg/add-column db dataset table attribute-name attribute-value)
      (catch java.sql.SQLException e
        (log/with-logs ['pdok.featured.projectors :error :error] (j/print-sql-exception-chain e))))))

(defn- feature-to-sparse-record [proj-fn feature all-fields-constructor import-nil-geometry?]
  ;; could use a valid-geometry? check?! For now not, as-jts return nil if invalid (for gml)
  (let [id (:id feature)
        version (:version feature)
        sparse-attributes (all-fields-constructor (:attributes feature))]
    (let [geometry (proj-fn (-> feature (:geometry) (f/as-jts)))]
      (when (or geometry import-nil-geometry?)
        (let [geo-group (f/geometry-group (:geometry feature))
              record (concat [id
                              version
                              (when (= :point geo-group) geometry)
                              (when (= :line geo-group) geometry)
                              (when (= :polygon geo-group) geometry)
                              geo-group]
                             sparse-attributes
                             [id])]
          record)))))

(defn- feature-keys [feature]
  (let [geometry (:geometry feature)
        attributes (:attributes feature)]
     (cond-> (transient [])
       (f/valid-geometry? geometry)
       (conj!-coll :_geometry_point :_geometry_line :_geometry_polygon :_geo_group)
       (seq attributes) (conj!-coll (keys attributes))
       true (persistent!))))

 (defn- feature-to-update-record [proj-fn feature]
   (let [attributes (:attributes feature)
         geometry (:geometry feature)
         geo-group (when geometry (f/geometry-group geometry))]
     (cond-> (transient [(:version feature)])
       (f/valid-geometry? geometry)
       (conj!-coll
        (when (= :point geo-group) (proj-fn (f/as-jts geometry)))
        (when (= :line geo-group) (proj-fn (f/as-jts geometry)))
        (when (= :polygon geo-group)  (proj-fn (f/as-jts geometry)))
        geo-group)
       (seq attributes) (conj!-coll (vals attributes))
       true (conj! (:id feature))
       true (conj! (:current-version feature))
       true (persistent!))))

(defn- all-fields-constructor [attributes]
  (if (empty? attributes) (constantly nil) (apply juxt (map #(fn [col] (get col %)) attributes))))

(defn- geo-column [geo-group]
   (str "_geometry_" (name geo-group)))

(defn gs-insert-sql [schema table columns]
  (str "INSERT INTO " (pg/quoted schema) "." (pg/quoted table)
       " ("  (str/join "," (map #(pg/quoted (name %)) columns)) ")"
       " SELECT " (str/join "," (repeat (count columns) "?"))
       " WHERE NOT EXISTS (SELECT 1 FROM " (pg/quoted schema) "." (pg/quoted table) " WHERE _id = ?)" ))

(defn- gs-add-feature
  ([{:keys [db dataset import-nil-geometry?]} proj-fn all-attributes-fn no-insert features]
   (try
     (let [filtered-features (filter #(not (@no-insert (:version %))) features)
           selector (juxt :collection)
           per-collection (group-by selector filtered-features)]
        (doseq [[[collection] grouped-features] per-collection]
         (let [all-attributes (all-attributes-fn collection)
               records (filter (comp not nil?)
                               (map #(feature-to-sparse-record proj-fn % (all-fields-constructor all-attributes) import-nil-geometry?)
                                    grouped-features))]
           (if (not (empty? records))
             (let [fields (concat [:_id :_version :_geometry_point :_geometry_line :_geometry_polygon :_geo_group]
                                  all-attributes)
                   sql (gs-insert-sql dataset collection fields)]
               (j/execute! db (cons sql records) :multi? true :transaction? (:transaction? db)))))))
     (catch java.sql.SQLException e
       (log/with-logs ['pdok.featured.projectors :error :error] (j/print-sql-exception-chain e))))))


(defn- gs-update-sql [schema table columns]
  (str "UPDATE " (pg/quoted schema) "." (pg/quoted table)
       " SET \"_version\" = ?, " (str/join "," (map #(str (pg/quoted %) " = ?") columns))
       " WHERE \"_id\" = ? and \"_version\" = ?;"))

(defn- execute-update-sql [{:keys [db dataset]} collection columns update-vals]
  (try
    (let [table collection
          sql (gs-update-sql dataset table (map name columns))]
      (j/execute! db (cons sql update-vals) :multi? true :transaction? (:transaction? db)))
    (catch java.sql.SQLException e
      (log/with-logs ['pdok.featured.projectors :error :error] (j/print-sql-exception-chain e)))))

(defn- gs-update-feature [{:keys [db dataset] :as gs} proj-fn features]
    (let [selector (juxt :collection)
          per-collection
           (group-by selector features)]
     (doseq [[[collection] collection-features] per-collection]
        ;; group per key collection so we can batch every group
        (let [keyed (group-by feature-keys collection-features)]
          (doseq [[columns vals] keyed]
            (when (< 0 (count columns))
              (let [update-vals (map (partial feature-to-update-record proj-fn) vals)]
                 (execute-update-sql gs collection columns update-vals)
               )))))))


(defn- gs-delete-sql [schema table]
  (str "DELETE FROM " (pg/quoted schema) "." (pg/quoted table)
       " WHERE \"_id\" = ? and \"_version\" = ?"))

(defn- gs-delete-feature [{:keys [db dataset]} features]
  (try
    (let [selector (juxt :collection)
          per-collection (group-by selector features)]
       (doseq [[[collection] collection-features] per-collection]
        (let [table collection
              sql (gs-delete-sql dataset table)
              ids (map #(vector (:id %1) (:current-version %1)) collection-features)]
          (j/execute! db (cons sql ids) :multi? true :transaction? (:transaction? db)))))
    (catch java.sql.SQLException e
      (log/with-logs ['pdok.featured.projectors :error :error] (j/print-sql-exception-chain e)))))

(defn- make-flush-all [proj-fn cache insert-batch update-batch delete-batch no-insert]
  "Used for flushing all batches, so entry order is alway new change close"
  (fn [gs]
    (fn []
      (let [cached-collection-attributes (cached cache gs-collection-attributes gs)]

        (process-batch update-batch (partial gs-update-feature gs proj-fn))
        (process-batch delete-batch (partial gs-delete-feature gs))

        (flush-batch insert-batch (partial gs-add-feature gs proj-fn cached-collection-attributes no-insert))
        (flush-batch update-batch (partial gs-update-feature gs proj-fn))
        (flush-batch delete-batch (partial gs-delete-feature gs))

        (dosync (vreset! no-insert #{}))
        ))))

(defn- versions-in-batch [{:keys [collection id]} batch]
   (map :version
        (filter (fn [f] (and
                     (= (:collection f) collection)
                     (= (:id f) id)))
                batch)))

(defrecord GeoserverProjector [db dataset cache insert-batch insert-batch-size
                             update-batch update-batch-size
                             delete-batch delete-batch-size no-insert
                             make-flush-fn flush-fn proj-fn no-visualization ndims srid
                               import-nil-geometry?]
  proj/Projector
  (proj/init [this for-dataset current-collections]
    (let [inited (assoc this :dataset for-dataset)
          flush-fn (make-flush-fn inited)
          ready (assoc inited :flush-fn flush-fn)]
      (when (not (gs-dataset-exists? ready))
        (checked (gs-create-dataset ready)
                 (gs-dataset-exists? ready)))
      ready))
  (proj/new-collection [this collection parent-collection])
  (proj/flush [this]
    (flush-fn)
    this)
  (proj/new-feature [this feature]
    (if (proj/accept? this feature)
      (let [{:keys [collection attributes]} feature
            cached-collection-exists? (cached cache gs-collection-exists? this)
            cached-collection-attributes (cached cache gs-collection-attributes this)
            batched-add-feature
            (batched insert-batch insert-batch-size flush-fn)]
        (do (when (not (cached-collection-exists? collection))
              (checked (gs-create-collection this ndims srid collection)
                       (gs-collection-exists? this collection))
              (cached-collection-exists? :reload collection))
            (let [current-attributes (cached-collection-attributes collection)
                  new-attributes (filter #(not (get current-attributes (first %))) attributes)]
              (doseq [[attr-key attr-value] new-attributes]
                (checked (gs-add-attribute this collection attr-key attr-value)
                         (get (gs-collection-attributes this collection) attr-key)))
              (when (not-empty new-attributes) (cached-collection-attributes :reload collection)))
            (batched-add-feature feature)))))
  (proj/change-feature [this feature]
    (if (proj/accept? this feature)
      (let [{:keys [collection attributes]} feature
            cached-collection-attributes (cached cache gs-collection-attributes this)
            batched-update-feature (batched update-batch update-batch-size flush-fn)]
        (let [current-attributes (cached-collection-attributes collection)
              new-attributes (filter #(not (get current-attributes (first %))) attributes)]
          (doseq [[attr-key attr-value] new-attributes]
            (checked (gs-add-attribute this collection attr-key (type attr-value))
                     (get (gs-collection-attributes this collection) attr-key)))
          (when (not-empty new-attributes) (cached-collection-attributes :reload collection)))
        (batched-update-feature feature))))
  (proj/close-feature [this feature]
    (if (proj/accept? this feature)
      (let [batched-delete-feature (batched delete-batch delete-batch-size flush-fn)]
        (batched-delete-feature feature))))
  (proj/delete-feature [this feature]
    (if (proj/accept? this feature)
      (let [versions-in-insert (versions-in-batch feature @insert-batch)
            batched-delete-feature (batched delete-batch delete-batch-size flush-fn)]
        (dosync
          (vswap! no-insert #(clojure.set/union % (into #{} versions-in-insert))))
        (batched-delete-feature feature))))
  (proj/accept? [_ feature]
         (not (some #{(:collection feature)} no-visualization)))
  (proj/close [this]
    (proj/flush this)
    this))


(defn geoserver-projector [config]
  (let [db (:db-config config)
        dataset "unknown-dataset"
        cache (volatile! {})
        insert-batch-size (or (:insert-batch-size config) (:batch-size config) 10000)
        insert-batch (volatile! (PersistentQueue/EMPTY))
        update-batch-size (or (:update-batch-size config) (:batch-size config) 10000)
        update-batch (volatile! (PersistentQueue/EMPTY))
        delete-batch-size (or (:delete-batch-size config) (:batch-size config) 10000)
        delete-batch (volatile! (PersistentQueue/EMPTY))
        no-insert (volatile! #{})
        ndims (or (:ndims config) 2)
        srid (or (:srid config) 28992)
        proj-fn (or (:proj-fn config) identity)
        no-visualization (:no-visualization config)
        make-flush-fn (make-flush-all proj-fn cache insert-batch update-batch delete-batch no-insert)
        import-nil-geometry? (:import-nil-geometry? config)]
    (->GeoserverProjector db dataset cache insert-batch insert-batch-size
                          update-batch update-batch-size
                          delete-batch delete-batch-size no-insert
                          make-flush-fn (fn []) proj-fn no-visualization ndims srid
                          import-nil-geometry?)))
