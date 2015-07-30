(ns pdok.featured.extracts
  (:require [clojure.edn :as edn]
            [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [pdok.featured.config :as config]
            [pdok.featured.json-reader :as json-reader]
            [pdok.featured.mustache  :as m]
            [pdok.featured.tiles :as tiles]
            [pdok.postgres :as pg]))


(def ^{:private true } extract-schema "extracten")

(defn- template [templates-dir dataset feature-type]
  (let [template-file (clojure.java.io/as-file (str templates-dir "/" dataset "/" feature-type ".mustache"))]
    (if (.exists template-file)
      {:name (str feature-type) :template (slurp template-file)}
      nil)))

(defn- partials [templates-dir dataset]
  (let [partials-dir (clojure.java.io/as-file (str templates-dir "/" dataset "/partials"))]
    (if (.exists partials-dir)
      (let [files (file-seq partials-dir)
            partials (filter #(.endsWith (.getName %) ".mustache") files)]
        (reduce (fn [acc val] (assoc acc
                                    (keyword (clojure.string/replace (.getName val) ".mustache" ""))
                                    (slurp val))) {} partials))
      nil)))

(defn features-for-extract [dataset feature-type features templates-dir]
  "Returns the rendered representation of the collection of features for a given feature-type inclusive tiles-set"
  (let [template (template templates-dir dataset feature-type)
        partials (partials templates-dir dataset)]
    (map #(vector (tiles/nl (:geometry %)) (m/render template % partials)) features)))

(defn create-extract-collection [db table]
    (do (when (not (pg/schema-exists? db extract-schema))
          (pg/create-schema db extract-schema))
        (when (not (pg/table-exists? db extract-schema table))
          (pg/create-table db extract-schema table
                     [:id "bigserial" :primary :key]
                     [:feature_type "text"]
                     [:valid_from "timestamp without time zone"]
                     [:valid_to "timestamp without time zone"]
                     [:tiles "text"]
                     [:xml "text"]
                     [:created_on "timestamp without time zone"]
                     )
          )))

(defn- jdbc-insert
  ([db table valid-from valid-to tiles xml created_on]
   (jdbc-insert db table (list [table valid-from valid-to tiles xml created_on])))
  ([db table entries]
   (try (j/with-db-connection [c db]
          (apply
           (partial j/insert! c (str extract-schema "." table) :transaction? false
                    [:table :valid_from :valid_to :tiles :xml :created_on])
           entries)
            )
        (catch java.sql.SQLException e (j/print-sql-exception-chain e)))))


(defn add-extract-records [dataset feature-type extract-type version rendered-features]
  "Inserts the xml-features and tile-set in an extract schema based on dataset, extract-type, version and feature-type,
   if schema or table doesn't exists it will be created."
   (let [table (str dataset "_" extract-type "_v" version "_" feature-type)]
  (do
   (create-extract-collection config/data-db table)
   (doseq [[tiles xml-feature] rendered-features]
     (jdbc-insert config/data-db table nil nil (vec tiles) xml-feature nil)))))


(defn file-to-features [path dataset]
  "Helper function to read features from a file.
   Returns features read from file."
  (with-open [s (json-reader/file-stream path)]
   (doall (json-reader/features-from-stream s :dataset dataset))))




;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
