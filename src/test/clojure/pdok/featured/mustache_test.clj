(ns pdok.featured.mustache-test
   (:require [clojure.java.io :as io]
             [clojure.test :refer :all]
             [cheshire.core :as json]
             [pdok.featured.mustache :refer :all]))

(def testMapping {:begin "eerste stap", :eind "tweede stap"})
(def testTemplate "Dit is een {{begin}} en later nog een {{eind}}")

(def resultTemplateMapping "Dit is een eerste stap en later nog een tweede stap")


(def testMapping2 {:begin "aaa", :eind "bbb"})
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
  { :traffic-area-gml-id "abcdef"
    :creation-data "2014-12-05"
    "LV-publicatiedatum" "2015-02-05T17:22:59.000"
    "tijdstipRegistratie" "2014-12-05T15:32:38.000"
    "inOnderzoek" false
    "relatieveHoogteligging" 0
    "bronhouder" "G0303"
    "lokaalID" "G0303.0979f33001fd319ae05332a1e90a5e0b"
    "status" "bestaand"
    "plus-status" "geenWaarde"
    "functie" "voetpad"
    "fysiekVoorkomen" "open verharding"
    "kruinlijn-leeg" "geenWaarde"
    "opTalud" false
    "plus-fysiekVoorkomen" "tegels"})

(def example-feature-bgt-wegdeel 
                  (merge
                    {:_action "new"}
    ;;                {:geometry example-geometry-bgt-wegdeel}
                     example-attributes-bgt-wegdeel))

(defn example-features [n] (repeat n example-feature-bgt-wegdeel))


 (defn render-wegdeel-with-example [n] 
   (render-template 
     "pdok/featured/templates/bgt-wegdeel.template" 
     (example-features n)))
  
 (deftest test-features-mapping
   (is (= 5 (count(filter #(re-find #"G0303.0979f33001fd319ae05332a1e90a5e0b" %) (render-wegdeel-with-example 5))))))

 (deftest test-features-gml-mapping
   (is (= 5 (count(filter #(re-find #"<gml:posList" %) (render-wegdeel-with-example 5))))))
 
 (deftest test-5-features-5
   (is (= 0 (count (filter #(re-find #"not be found" %) (render-wegdeel-with-example 5))))))

 (defn write-gml-files [n]
    (time (with-open [w (clojure.java.io/writer "target/features.gml.json")]
       (json/generate-stream {:features (render-wegdeel-with-example n)} w))))
  
 (def test-map {:Mutatie "Iets" :AndereMutatie "EnIetsAnders"})
 
         

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s)))))
