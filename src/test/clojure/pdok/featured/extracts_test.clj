(ns pdok.featured.extracts-test
   (:require [pdok.featured.json-reader :refer [features-from-stream file-stream]]
             [pdok.featured.extracts :refer :all]
             [pdok.featured.feature :as f]
             [pdok.featured.config :as config]
             [pdok.featured.projectors :as p]
             [clojure.test :refer :all]
             [clojure.java.io :as io]))

(defn- test-feature [name something other]
              {"name" name
               "something" something
               "other" other
               "_geometry" {"type" "gml", "gml" "<gml:Polygon xmlns:gml=\"http://www.opengis.net/gml\" srsName=\"EPSG:28992\"><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">10.0 10.0 5.0 10.0 5.0 5.0 10.0 5.0 10.0 10.0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon>"}})

(defn one-feature [] (list (test-feature "PDOK" "AAA" "BBB")))

(defn two-features [] (list (test-feature "name1" "A" "B")
                             (test-feature "name2" "C" "D")))

(deftest test-two-rendered-features 
  (let [[error features-for-extract] (features-for-extract "test" "dummy" "gml2extract" (two-features) "src/test/resources/templates")]
    (is (= 2 )(count features-for-extract))))

(deftest test-rendered-feature-gml
  (let [[error features-for-extract] (features-for-extract "test" "dummy" "gml2extract" (one-feature) "src/test/resources/templates")
        [tiles result-feature] (first features-for-extract)]
    (is (boolean (re-find #"<geo><gml:Polygon" result-feature)))
    (is (boolean (re-find #"<naam>PDOK</naam>" result-feature)))))

(def ^{:private true} extract-type-citygml "citygml")

(defn write-xml-to-database [dataset feature-type path template-dir]
  "Helper function to write features to an extract-schema"
  (let [features (file-to-features path dataset)
       [error features-for-extract] (features-for-extract dataset feature-type extract-type-citygml features template-dir)]
    (add-extract-records config/data-db dataset feature-type extract-type-citygml 15 features-for-extract)))

; (write-xml-to-database "bgt" "bord" "D:\\data\\pdok\\bgt\\mutatie-leveringen\\bord\\973140-Bord-1.json" "D:\\projects\\featured\\src\\main\\resources\\pdok\\featured\\templates")

(defn test-fill-extract []
  (fill-extract "bgt" "bord" "citygml" "33"))


;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
