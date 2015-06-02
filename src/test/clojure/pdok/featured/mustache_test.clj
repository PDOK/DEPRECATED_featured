(ns pdok.featured.mustache-test
   (:require [clojure.java.io :as io]
             [clojure.test :refer :all]
             [cheshire.core :as json]
             [pdok.featured.mustache :refer :all]))

(def testMapping {"begin" "eerste stap", "eind" "tweede stap"})
(def testTemplate "Dit is een {{begin}} en later nog een {{eind}}")

(def resultTemplateMapping "Dit is een eerste stap en later nog een tweede stap")

(def testMapping2 {"begin" "aaa", "eind" "bbb"})
(def testTemplate2 "Eerst iets anders {{begin}} en op het einde {{eind}}")
(def resultTemplate2Mapping2 "Eerst iets anders aaa en op het einde bbb")

(def resultTemplate2Mapping "Eerst iets anders eerste stap en op het einde tweede stap")

(deftest replace-template-with-different-templates-and-mappings
  (is (= resultTemplateMapping (render-template testTemplate (->MapProxy testMapping))))
  (is (= resultTemplate2Mapping2 (render-template testTemplate2 (->MapProxy testMapping2))))
  (is (= resultTemplate2Mapping (render-template testTemplate2 (->MapProxy testMapping)))))


(def example-gml-bgt-wegdeel
  "<gml:Surface srsName=\"urn:ogc:def:crs:EPSG::28992\"><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\" count=\"5\">172307.599 509279.740 172307.349 509280.920 172306.379 509280.670 172306.699 509279.490 172307.599 509279.740</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>"
   )
 
(def example-geometry-bgt-wegdeel {"gml" example-gml-bgt-wegdeel, "type" "gml"})

(def example-attributes-bgt-wegdeel
  { "_traffic-area-gml-id" "abcdef"
    "_creation-data" "2014-12-05"
    "LV-publicatiedatum" "2015-02-05T17:22:59.000"
    "tijdstipRegistratie" "2014-12-05T15:32:38.000"
    "inOnderzoek" false
    "relatieveHoogteligging" 0
    "bronhouder" "G0303"
    "lokaalID" "G0303.0979f33001fd319ae05332a1e90a5e0b"})

(def example-feature-bgt-wegdeel 
                  (merge
                    {"_action" "new"}
                    {"_geometry" example-geometry-bgt-wegdeel}
                     example-attributes-bgt-wegdeel))

(defn example-features [n] (repeat n example-feature-bgt-wegdeel))

(defn render-wegdeel-with-example [n] 
   (render-template 
     "pdok/featured/templates/bgt-wegdeel.template" 
     (example-features n)))

  
 (deftest test-features-mapping
   (is (= 5 (count(filter #(re-find #"G0303.0979f33001fd319ae05332a1e90a5e0b" %) (render-wegdeel-with-example 5)))))
   (is (= 5 (count(filter #(re-find #"<imgeo:inOnderzoek>false</imgeo:inOnderzoek>" %) (render-wegdeel-with-example 5))))))

 
 (deftest test-features-gml-mapping
   (is (= 5 (count(filter #(re-find #"<gml:posList" %) (render-wegdeel-with-example 5))))))
 
 (deftest test-5-features-5
   (is (= 0 (count (filter #(re-find #"not be found" %) (render-wegdeel-with-example 5))))))


(def example-attributes-bgt-wegdeel-with-default
  { "_traffic-area-gml-id" "abcdef"
    "_creation-data" "2014-12-05"
    "inOnderzoek" nil
    "inOnderzoek_leeg" true
    "relatieveHoogteligging" 0
    "bronhouder" nil
    "bronhouder_leeg" "BGT"
    "lokaalID" "G0303.0979f33001fd319ae05332a1e90a5e0b"})

(def example-feature-bgt-wegdeel-with-default 
      (merge {"_action" "new"}
             {"_geometry" nil}
             {"_geometry_leeg" "NO GEO"}
             example-attributes-bgt-wegdeel-with-default))

(defn example-features-with-default [n] (repeat n example-feature-bgt-wegdeel-with-default))

(defn render-wegdeel-with-example-with-default [n] 
   (render-template 
     "pdok/featured/templates/bgt-wegdeel.template" 
     (example-features-with-default n)))

(deftest test-features-mapping-with-default 
  (let [rendered-template (first (render-wegdeel-with-example-with-default 1))]
    (is (= 1 (count (re-seq #"NO GEO" rendered-template))))
    (is (= 1 (count (re-seq #"<imgeo:bronhouder>BGT</imgeo:bronhouder>" rendered-template))))
    (is (= 1 (count (re-seq #"<imgeo:inOnderzoek>true</imgeo:inOnderzoek>" rendered-template))))
    (is (= 13 (count (re-seq #"NO VALUE" rendered-template))))
    (is (= 1 (count (re-seq #"G0303.09" rendered-template))))))

 (defn write-gml-files [n]
    (time (with-open [w (clojure.java.io/writer "target/features.gml.json")]
       (json/generate-stream {:features (render-wegdeel-with-example n)} w))))
  
 (deftest test-split-feature-key-nested-with-function
   (let [feature-key "wegdeel.kruinlijn.geo.#gml"]
     (is (= {:template-keys ["wegdeel" "kruinlijn" "geo"]
             :template-function "gml"} 
            (split-feature-key feature-key)))))
  
 (deftest test-split-feature-key-nested-without-function
   (let [feature-key "wegdeel.kruinlijn.geo"]
     (is (= {:template-keys ["wegdeel" "kruinlijn" "geo"]
             :template-function nil} 
            (split-feature-key feature-key)))))
 
 (deftest test-split-feature-key-nested-with-function
   (let [feature-key "wegdeel.kruinlijn.geo.#gml"]
     (is (= {:template-keys ["wegdeel" "kruinlijn" "geo"]
             :template-function "gml"} 
            (split-feature-key feature-key)))))
  
 (deftest test-split-feature-key-with-function
   (let [feature-key "wegdeel.geo.#gml"]
     (is (= {:template-keys ["wegdeel" "geo"]
             :template-function "gml"} 
            (split-feature-key feature-key)))))
 
 (deftest test-split-simple-feature-key
   (let [feature-key "wegdeel"]
     (is (= {:template-keys ["wegdeel"]
             :template-function nil} 
            (split-feature-key feature-key)))))
 
 (deftest test-split-simple-feature-key-with-keys
   (let [feature-key "wegdeel"]
     (is (= {:template-keys ["wegdeel"]
             :template-function nil} 
            (split-feature-key feature-key)))))
 
 
 
 
         

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s)))))
