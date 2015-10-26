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

(deftest test-two-rendered-features 
  (let [[error features] (features-for-extract "test" "dummy" "gml2extract" (two-features) "src/test/resources/templates")]
    (is (= 2 )(count features))))

(def ^{:private true} extract-type-citygml "citygml")

(defn write-xml-to-database [dataset feature-type path template-dir]
  "Helper function to write features to an extract-schema."
  (let [features (file-to-features path dataset)
       [error features-for-extract] (features-for-extract dataset feature-type extract-type-citygml features template-dir)]
    (add-extract-records config/data-db dataset feature-type extract-type-citygml 14 features-for-extract)))


(def test-partial "<gml:start en nog wat namespaces>")
(def test-template "{{>model-start}}<imgeo:Bak>{{>start-feature-type}}<imgeo:geometrie2dBak>{{{_geometry.gml}}}</imgeo:geometrie2dBak></imgeo:Bak>{{>model-eind}}")
(def expected-template "{{>bgt-city-model-start}}<imgeo:Bak>{{>bgt-city-start-feature-type}}<imgeo:geometrie2dBak>{{{_geometry.gml}}}</imgeo:geometrie2dBak></imgeo:Bak>{{>bgt-city-model-eind}}")

(deftest test-replace-in-template
  (is (= expected-template (replace-in-template test-template "bgt" "city" "{{>"))))


(def test-store (create-template-store))

(deftest test-add-or-update-template-store
  (do 
    (add-or-update-template-store test-store "bgt" "city" "bak" false test-template)
    (add-or-update-template-store test-store "bgt" "gml-light" "bak" false test-template)
    (add-or-update-template-store test-store "bgt" "city" "start-bgt" true test-partial))
    (is (= 2 (count (get-in @(:templates test-store) ["bgt"]))))
    (is (= 1 (count (get-in @(:partials test-store) ["bgt"]))))
    (is (= expected-template (get-in @(:templates test-store) ["bgt" "city" "bak"])))
  )


;(write-xml-to-database "bgt" "bord" "D:\\data\\pdok\\bgt\\mutatie-leveringen\\bord\\973140-Bord-1.json" "D:\\projects\\featured\\src\\main\\resources\\pdok\\featured\\templates")

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
