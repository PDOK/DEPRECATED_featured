(ns pdok.featured.extracts-test
   (:require [pdok.featured.json-reader :refer [features-from-stream file-stream]]
             [pdok.featured.extracts :refer :all]
             [pdok.featured.template :as template]
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
  (let [_ (template/add-or-update-template {:dataset "test"
                                            :extract-type "gml2extract"
                                            :name "dummy"
                                            :template test-gml2extract-dummy-template})
        _ (template/add-or-update-template {:dataset "test"
                                            :extract-type "gml2extract"
                                            :name "start"
                                            :template test-gml2extract-start-partial})
        _ (template/add-or-update-template {:dataset "test"
                                            :extract-type "gml2extract"
                                            :name "end"
                                            :template test-gml2extract-end-partial})
        [error features] (features-for-extract "test" "dummy" "gml2extract" (two-features))
        rendered-feature (nth (first features) 2)]
    (is (= 2 (count features)))
    (is (= test-expected-rendered-feature rendered-feature))))

(def ^{:private true} extract-type-citygml "citygml")

(defn write-xml-to-database [dataset feature-type path template-dir]
  "Helper function to write features to an extract-schema."
  (let [features (file-to-features path dataset)
       [error features-for-extract] (features-for-extract dataset feature-type extract-type-citygml features template-dir)]
    (add-extract-records config/extracts-db dataset feature-type extract-type-citygml 14 features-for-extract)))


;; The -elem-at- construction can be used in mustache templates to get an element from a specific position in a list. For instance elem-at-1 returns the second element in list.
;; In the example template 3 elements are used. In the example input data there are two elements in "nummeraanduidingreeks" present. In the output only these two elements can be found.
(def elem-at-inputdata  (file-to-features (io/resource "templates/test/elemat/bgt_pand.json") "bgtmutatie" ))
(def test-indexed-section (slurp (io/resource "templates/test/elemat/indexedsection.mustache")))
(def elem-at-expectedoutput "<imgeo-s:Pand><elem1>1111111111111111<hoek1>A</hoek1><hoek2>B</hoek2></elem1><elem2>9999999999999999<hoek3>C</hoek3></elem2></imgeo-s:Pand>")
(deftest test-elem-at
  (let [_ (template/add-or-update-template {:dataset "bgtmutatie"
                                            :extract-type "testing"
                                            :name "indexedsection"
                                            :template test-indexed-section})
        [error features] (features-for-extract "bgtmutatie" "indexedsection" "testing" elem-at-inputdata)]
  (is (= elem-at-expectedoutput (clojure.string/replace (nth (first features) 2) " " "")))))


;(write-xml-to-database "bgt" "bord" "D:\\data\\pdok\\bgt\\mutatie-leveringen\\bord\\973140-Bord-1.json" "D:\\projects\\featured\\src\\main\\resources\\pdok\\featured\\templates")

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
