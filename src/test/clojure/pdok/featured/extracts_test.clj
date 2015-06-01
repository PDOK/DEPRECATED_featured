(ns pdok.featured.extracts-test
   (:require [pdok.featured.json-reader :refer [features-from-stream file-stream]]
             [pdok.featured.extracts :refer :all]
             [clojure.test :refer :all]
             [clojure.java.io :as io]))

(defn- test-feature [name something other]
              {"name" name
               "something" something
               "other" other
               :geometry {"type" "gml", "gml" "<gml:Polygon xmlns:gml=\"http://www.opengis.net/gml\" srsName=\"EPSG:28992\"><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">10.0 10.0 5.0 10.0 5.0 5.0 10.0 5.0 10.0 10.0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>"}})

(defn- one-feature [] (list (test-feature "PDOK" "AAA" "BBB")))

(defn- two-features [] (list (test-feature "name1" "A" "B") 
                             (test-feature "name2" "C" "D")))

(deftest test-two-template-features
  (is (= 2 )(count (template-features "test" "dummy" (two-features)))))

(deftest test-template-feature-resolve-gml
  (let [result-feature (first (template-features "test" "dummy" (one-feature)))]
    (is (boolean (re-find #"<geo><gml:Polygon" result-feature)))
    (is (boolean (re-find #"<naam>PDOK</naam>" result-feature)))))


(defn- file-to-features [path dataset] 
  (with-open [s (file-stream path)]
   (doall (features-from-stream s :dataset dataset))))

(defn- write-feature [gml-from-feature file]
  (with-open [w (clojure.java.io/writer file)]
     (.write w gml-from-feature)))

(defn write-xml [dataset feature-type path]
  "Helper function to write template-features to filesystem."
  (let [features (file-to-features path dataset)
        template-features (template-features dataset feature-type features)]
     (doseq [ [idx template-feature] (map #(vector %1 %2) (range) template-features)] 
               (write-feature template-feature (str "target/output_" idx ".xml")))))


;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))