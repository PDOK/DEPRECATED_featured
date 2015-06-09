(ns pdok.featured.extracts
   (:require [pdok.featured.mustache  :as m]))

(defn- template-file [dataset feature-type]
  (str "pdok/featured/templates/" dataset "-" feature-type ".template"))

(defn render-features [dataset feature-type features]
  "Returns the rendered representation of the collection of features for a the given feature-type,
   based on the feature-type and the dataset a (gml-)template will be used"
  (let [template (template-file dataset feature-type)]
    (map (partial m/render-resource template) features)))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))