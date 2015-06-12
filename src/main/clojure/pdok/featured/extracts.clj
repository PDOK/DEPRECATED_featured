(ns pdok.featured.extracts
   (:require [pdok.featured.mustache  :as m]
             [pdok.postgres :as pg]
             [pdok.featured.projectors :as p]
             [pdok.featured.core :as core]
             [clojure.java.jdbc :as j]))

(defn- template-file [dataset feature-type]
  (str "pdok/featured/templates/" dataset "-" feature-type ".template"))

(defn features-for-extract [dataset feature-type features]
  "Returns the rendered representation of the collection of features for a the given feature-type inclusive tiles-set"
  (let [template (template-file dataset feature-type)]
    (map #(vector (p/nl-tiles %) (m/render-resource template %)) features)))

(defn create-extract-collection [db dataset feature-type]
  (let [table feature-type]
    (do (when (not (pg/schema-exists? db dataset))
          (pg/create-schema db dataset))
        (when (not (pg/table-exists? db dataset table))
          (pg/create-table db dataset feature-type
                     [:id "bigserial" :primary :key]
                     [:feature_type "text"]
                     [:valid_from "timestamp without time zone"]
                     [:valid_to "timestamp without time zone"]
                     [:tiles "text"]
                     [:xml "text"]
                     [:created_on "timestamp without time zone"]
                     )
          ))))


(defn- jdbc-insert
  ([db feature-type valid-from valid-to tiles xml created_on]
   (jdbc-insert db feature-type (list [feature-type valid-from valid-to tiles xml created_on])))
  ([db feature-type entries]
   (try (j/with-db-connection [c db]
          (apply
           (partial j/insert! c (str "bgtextract." feature-type) :transaction? false
                    [:feature_type :valid_from :valid_to :tiles :xml :created_on])
           entries)
            )
        (catch java.sql.SQLException e (j/print-sql-exception-chain e)))))


(defn add-extract-records [dataset feature-type rendered-features]
  "Inserts the xml-features and tile-set in an extract schema based on dataset and feature-type,
   if schema or table doesn't exists it will be created."
  (do
   (create-extract-collection core/data-db (str dataset "extract") feature-type)
   (doseq [[tiles xml-feature] rendered-features]
     (jdbc-insert core/data-db feature-type nil nil (vec tiles) xml-feature nil))))

  
  
;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))