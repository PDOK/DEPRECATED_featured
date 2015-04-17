(ns pdok.featured.geometry-test
   (:require [clojure.java.io :as io]
             [clojure.test :refer :all]
             [pdok.featured.geometry :refer :all]))

(def testMapping {:begin "eerste stap", :eind "tweede stap"})
(def testTemplate "Dit is een {{begin}} en later nog een {{eind}}")

(def resultTemplateMapping "Dit is een eerste stap en later nog een tweede stap")


(def testMapping2 {:begin "aaa", :eind "bbb"})
(def testTemplate2 "Eerst iets anders {{begin}} en op het einde {{eind}}")
(def resultTemplate2Mapping2 "Eerst iets anders aaa en op het einde bbb")

(def resultTemplate2Mapping "Eerst iets anders eerste stap en op het einde tweede stap")


(def example-gml-bgt-wegdeel
  "<gml:Surface srsName=\"urn:ogc:def:crs:EPSG::28992\"><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\" count=\"5\">172307.599 509279.740 172307.349 509280.920 172306.379 509280.670 172306.699 509279.490 172307.599 509279.740</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>"
   )
 
(def example-geometry-bgt-wegdeel {"gml" example-gml-bgt-wegdeel, "type" "gml"})

(def example-attributes-bgt-wegdeel
  { :traffic-area-gml-id "abcdef"
    :creation-data "2014-12-05"
    :publicatie-datum "2015-02-05T17:22:59.000"
    :tijdstip-regristatie "2014-12-05T15:32:38.000"
    :in-onderzoek false
    :relatieve-hoogteligging "0"
    :bronhouder "G0303"
    :imgeo-namespace "NL.IMGeo"
    :imgeo-lokaalid "G0303.0979f33001fd319ae05332a1e90a5e0b"
    :bgt-status "bestaand"
    :plus-status "geenWaarde"
    :function "voetpad"
    :surface-material "open verharding"
    :kruinlijn-wegdeel-nil-reason "geenWaarde"
    :wegdeel-op-talud false
    :plus-fysiek-voorkomen-wegdeel "tegels"})

(def example-feature-bgt-wegdeel 
                  {:_action "new"
                   :geometry example-geometry-bgt-wegdeel
                   :attributes example-attributes-bgt-wegdeel})

(deftest replace-template-with-different-templates-and-mappings
  (is (= resultTemplateMapping (replace-template testTemplate (->MapProxy {:attributes testMapping}))))
  (is (= resultTemplate2Mapping2 (replace-template testTemplate2 (->MapProxy {:attributes testMapping2}))))
  (is (= resultTemplate2Mapping (replace-template testTemplate2 (->MapProxy {:attributes testMapping})))))


(deftest bgt-wegdeel-template-to-pattern
   (println (template-to-pattern (read-template "pdok/featured/templates/bgt-wegdeel.template")))
  )

 (deftest bgt-replace
  (let [ template (read-template "pdok/featured/templates/bgt-wegdeel.template")
         _ (println template)
         template-replaced (replace-template template (->MapProxy example-feature-bgt-wegdeel))]
    (println template template-replaced)))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
