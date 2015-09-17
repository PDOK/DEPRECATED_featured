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
        [nil (map #(vector feature-type (:_tiles %) (m/render template % partials)
                           (:_valid_from %) (:_valid_to %)) features)]))))

(defn- jdbc-insert-extract [db table entries]
   (try (j/with-db-connection [c db]
          (apply (partial j/insert! c (str extract-schema "." table) :transaction? false
                    [:feature_type :valid_from :valid_to :tiles :xml])
                    entries))
        (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(defn get-or-add-extractset [db extractset version]
  "return id"
  (let [query (str "select id from " extractset-table " where extractset_name = ? and version = ?")]
    (j/with-db-connection [c db]
      (let [result (j/query c [query extractset version])]
        (if (empty? result)
          (do
            (j/query c [(str "SELECT " extract-schema ".add_extractset(?,?)") extractset (int version)])
            (get-or-add-extractset db extractset version))

          (:id (first result)))))))

(defn add-extractset-area [db extractset-id tiles]
  (let [query (str "select * from " extractset-area-table " where extractset_id = ? and area_id = ?")]
    (j/with-db-transaction [c db]
      (doseq [area-id tiles]
        (if (empty? (j/query c [query extractset-id area-id]))
          (j/insert! db extractset-area-table {:extractset_id extractset-id :area_id area-id}))))))

(defn- tiles-from-feature [[type tiles & more]]
   tiles)

(defn add-metadata-extract-records [db extractset-id rendered-features]
   (let [tiles (reduce clojure.set/union (map tiles-from-feature rendered-features))]
     (add-extractset-area db extractset-id tiles)))

(defn- tranform-feature-for-db [[feature-type tiles xml-feature valid-from valid-to]]
  [feature-type valid-from valid-to (vec tiles) xml-feature])

(defn add-extract-records [db dataset feature-type extract-type version rendered-features]
  "Inserts the xml-features and tile-set in an extract schema based on dataset, extract-type, version and feature-type,
   if schema or table doesn't exists it will be created."
  (let [collection (str dataset "_" extract-type)
        table (str collection "_v" version)
        extractset-id (get-or-add-extractset db collection version) ]
    (do
      (jdbc-insert-extract db table (map tranform-feature-for-db rendered-features))
      (add-metadata-extract-records db extractset-id rendered-features))
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
