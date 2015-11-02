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

(defn two-features [] (list (test-feature "name1" "A" "B")
                             (test-feature "name2" "C" "D")))

(def test-expected-rendered-feature "<dummyObjectMember><naam><hier is een begin>name1</naam><ietsAnders object=\"A\">B</ietsAnders><geo><gml:Polygon xmlns:gml=\"http://www.opengis.net/gml\" srsName=\"EPSG:28992\"><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\">10.0 10.0 5.0 10.0 5.0 5.0 10.0 5.0 10.0 10.0</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></geo><noValue></noValue><dit lijkt wel een eind/></dummyObjectMember>")

(def test-gml2extract-dummy-template (slurp (io/resource "templates/test/gml2extract/dummy.mustache")))
(def test-gml2extract-start-partial (slurp (io/resource "templates/test/gml2extract/partials/start.mustache")))
(def test-gml2extract-end-partial (slurp (io/resource "templates/test/gml2extract/partials/end.mustache")))


(deftest test-two-rendered-features 
  (let [_ (add-or-update-template "test" "gml2extract" "dummy" test-gml2extract-dummy-template)
        _ (add-or-update-template "test" "gml2extract" "start" test-gml2extract-start-partial)
        _ (add-or-update-template "test" "gml2extract" "end" test-gml2extract-end-partial)
        [error features] (features-for-extract "test" "dummy" "gml2extract" (two-features))
        rendered-feature (nth (first features) 2)]
    (is (= 2 (count features)))
    (is (= test-expected-rendered-feature rendered-feature))))

(def ^{:private true} extract-type-citygml "citygml")

(defn write-xml-to-database [dataset feature-type path template-dir]
  "Helper function to write features to an extract-schema."
  (let [features (file-to-features path dataset)
       [error features-for-extract] (features-for-extract dataset feature-type extract-type-citygml features template-dir)]
    (add-extract-records config/data-db dataset feature-type extract-type-citygml 14 features-for-extract)))


;(write-xml-to-database "bgt" "bord" "D:\\data\\pdok\\bgt\\mutatie-leveringen\\bord\\973140-Bord-1.json" "D:\\projects\\featured\\src\\main\\resources\\pdok\\featured\\templates")

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
