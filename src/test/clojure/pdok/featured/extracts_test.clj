(ns pdok.featured.extracts-test
   (:require [pdok.featured.json-reader :refer [features-from-stream file-stream]]
             [pdok.featured.extracts :refer :all]
             [clojure.java.io :as io]))

(defn- file-to-features [path] 
  (with-open [s (file-stream path)]
   (doall(features-from-stream s :dataset "bgt"))))

(defn- write-feature [gml-from-feature file]
  (with-open [w (clojure.java.io/writer file)]
     (.write w gml-from-feature)))

(defn write-xml [dataset feature-type path]
  (let [features (file-to-features path)
        gml-features (gml-features dataset feature-type features)]
     (doseq [ [idx gml-feature] (map #(vector %1 %2) (range) gml-features)] 
               (write-feature gml-feature (str "output_" idx ".xml")))))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))