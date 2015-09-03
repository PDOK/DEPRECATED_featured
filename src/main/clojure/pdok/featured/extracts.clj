(ns pdok.featured.extracts
  (:require [clojure.edn :as edn]
            [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [pdok.featured.config :as config]
            [pdok.featured.json-reader :as json-reader]
            [pdok.featured.mustache  :as m]
            [pdok.featured.timeline :as timeline]
            [pdok.postgres :as pg]))


(def ^{:private true } extract-schema "extractmanagement")
(def ^{:private true } extractset-table "extractmanagement.extractset")
(def ^{:private true } extractset-area-table "extractmanagement.extractset_area")

(defn- template [templates-dir dataset feature-type extract-type]
  (let [template-file (clojure.java.io/as-file (str templates-dir "/" dataset "/" extract-type "/" feature-type ".mustache"))]
    (if (.exists template-file)
      {:name (str feature-type) :template (slurp template-file)}
      nil)))

(defn- partials [templates-dir dataset extract-type]
  (let [partials-dir (clojure.java.io/as-file (str templates-dir "/" dataset "/" extract-type "/partials"))]
    (if (.exists partials-dir)
      (let [files (file-seq partials-dir)
            partials (filter #(.endsWith (.getName %) ".mustache") files)]
        (reduce (fn [acc val] (assoc acc
                                    (keyword (clojure.string/replace (.getName val) ".mustache" ""))
                                    (slurp val))) {} partials))
      nil)))

(defn features-for-extract [dataset feature-type extract-type features templates-dir]
  "Returns the rendered representation of the collection of features for a given feature-type inclusive tiles-set"
  (if (empty? features) 
    [nil nil]
    (let [template (template templates-dir dataset feature-type extract-type)
          partials (partials templates-dir dataset extract-type)]
      (if (or (nil? template) (nil? partials))
        [(str "Template or partials cannot be found for dataset: " dataset 
                                                    " feature-type: " feature-type 
                                                    " extract-type: " extract-type 
                                                    " template-dir: " templates-dir) nil]
        [nil (map #(vector (:_tiles %) (m/render template % partials) (:_valid_from %) (:_valid_to %)) features)]))))


(defn create-extract-collection [db table]
    (do (when (not (pg/schema-exists? db extract-schema))
          (pg/create-schema db extract-schema))
        (when (not (pg/table-exists? db extract-schema table))
          (pg/create-table db extract-schema table
                     [:id "bigserial" :primary :key]
                     [:valid_from "timestamp without time zone"]
                     [:valid_to "timestamp without time zone"]
                     [:tiles "text"]
                     [:xml "text"]
                     [:created_on "timestamp without time zone"]
                     )
          )))

(defn- jdbc-insert-extract [db table entries]
   (try (j/with-db-connection [c db]
          (apply (partial j/insert! c (str extract-schema "." table) :transaction? false
                    [:valid_from :valid_to :tiles :xml :created_on])
                    entries))  
        (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(defn add-extractset [db extractset version]
  (let [query (str "select id from " extractset-table " where extractset_name = ? and version = ?")]
    (j/with-db-connection [c db]
      (let [result (j/query c [query extractset version])] 
        (if (empty? result)
          (j/insert! db extractset-table {:extractset_name extractset :version version :active false})
          result)))))

(defn add-extractset-area [db extractset-id tiles]
  (let [query (str "select * from " extractset-area-table " where extractset_id = ? and area_id = ?")]
    (j/with-db-transaction [c db]
      (doseq [area-id tiles]
        (if (empty? (j/query c [query extractset-id area-id]))
          (j/insert! db extractset-area-table {:extractset_id extractset-id :area_id area-id}))))))

(defn- tiles-from-feature [[tiles & more]]
   tiles)

(defn add-metadata-extract-records [db dataset extract-type version rendered-features]
   (let [extractset-id (:id (first (add-extractset db (str dataset "_" extract-type) version)))
         tiles (reduce clojure.set/union (map tiles-from-feature rendered-features))]
     (add-extractset-area db extractset-id tiles)))

(defn- tranform-feature-for-db [[tiles xml-feature valid-from valid-to]]
  [valid-from valid-to (vec tiles) xml-feature nil])

(defn add-extract-records [db dataset feature-type extract-type version rendered-features]
  "Inserts the xml-features and tile-set in an extract schema based on dataset, extract-type, version and feature-type,
   if schema or table doesn't exists it will be created."
  (let [table (str dataset "_" extract-type "_v" version "_" feature-type)]
    (do
      (create-extract-collection db table)
      (jdbc-insert-extract db table (map tranform-feature-for-db rendered-features))
      (add-metadata-extract-records db dataset extract-type version rendered-features))
    (count rendered-features)))

(defn fill-extract [dataset collection extract-type extract-version]
  (let [feature-type collection
        features (timeline/all (config/timeline) dataset collection)
        [error features-for-extract] (features-for-extract dataset 
                                                           feature-type 
                                                           extract-type 
                                                           features 
                                                           "src/main/resources/pdok/featured/templates")]
    (if (nil? error)
      (if (nil? features-for-extract)
        {:status "ok" :count 0}
        {:status "ok" :count (add-extract-records config/data-db dataset feature-type extract-type extract-version features-for-extract)})
      {:status "error" :msg error :count 0})))

(defn file-to-features [path dataset]
  "Helper function to read features from a file.
   Returns features read from file."
  (with-open [s (json-reader/file-stream path)]
   (doall (json-reader/features-from-stream s :dataset dataset))))




;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
