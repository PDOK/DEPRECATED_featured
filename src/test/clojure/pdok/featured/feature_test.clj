(ns pdok.featured.feature-test
  (:require [clojure.test :refer :all]
            [pdok.featured.feature :refer :all]
            [clojure.java.io :refer :all])
  (:import [pdok.featured.xslt TransformXSLT]
           [nl.pdok.gml3.impl GMLMultiVersionParserImpl]))


(def gml-with-hole (slurp (resource "gml/gml-with-hole.gml")))

(def gml-object-with-hole {"type" "gml" "gml" gml-with-hole})

(def gml-with-arc (slurp (resource "gml/gml-with-arc.gml")))
(def gml-object-with-arc {"type" "gml" "gml" gml-with-arc})

(def gml-without-curves (slurp (resource "gml/gml-without-curves.gml")))
(def gml-object-without-curves {"type" "gml" "gml" gml-without-curves})

(def gml-surface (slurp (resource "gml/gml-surface.gml")))

(def gml-one-curve (slurp (resource "gml/gml-one-curve.gml")))

(def gml-surface-with-more-elements (slurp (resource "gml/gml-surface-with-more-elements.gml")))
(def gml-object-surface-with-more-elements {"type" "gml" "gml" gml-surface-with-more-elements})

(def broken-gml (slurp (resource "gml/broken-gml.gml")))

(def bag-gml-in-rd (slurp (resource "gml/bag-polygon-rd.gml")))
(def bag-gml-in-etrs89 (slurp (resource "gml/bag-polygon-etrs89.gml")))

(def gml3-etrs-parser
  (GMLMultiVersionParserImpl. 0.01 4258))

(deftest test-as-jts-with-curves
 (is (re-find #"curve" (as-gml gml-object-with-hole)))
 (is (not (re-find #"curve" (as-gml gml-object-without-curves))))
 (is (= (as-jts gml-object-with-hole) (as-jts gml-object-without-curves))))

(deftest test-gml-one-curve
  (let [transformed-gml (-> gml-one-curve gml3-as-jts jts-as-wkt)]
    (is (re-find #"LINESTRING" transformed-gml))
    (is (re-find #"76492.094 453702.905" transformed-gml))))

(deftest test-gml-surface
  (let [transformed-gml (-> gml-surface gml3-as-jts jts-as-wkt)]
    (is (re-find #"POLYGON" transformed-gml))
    (is (re-find #"196284.313 391985.153" transformed-gml))
    (is (re-find #"196305.082 391991.976" transformed-gml))))

(deftest test-gml-with-more-elements
  (let [transformed-gml (-> gml-surface-with-more-elements gml3-as-jts jts-as-wkt)]
    (is (re-find #"POLYGON" transformed-gml))
    (is (re-find #"176567.478 317267.125" transformed-gml))))

(defn test-xslt [xslt document]
  (.transform
    (TransformXSLT.
       (clojure.java.io/input-stream
         (clojure.java.io/resource (str "pdok/featured/xslt/" xslt ".xsl"))))
   (slurp (resource (str "gml/" document ".gml")))))

(deftest test-broken-gml
  "Test converting a broken GML results in a nil geometry"
  (is (nil?  (gml3-as-jts broken-gml)))
  (is (nil?  (-> broken-gml gml3-as-jts jts-as-wkt))))

(defn gml3-in-etrs89-as-jts [gml]
    (.toJTSGeometry ^GMLMultiVersionParserImpl gml3-parser gml))

(deftest rd-to-etrs89-conversion
  "Test if a geometry is correctly transformed from/to RD to ETRS89"
  (is (= (-> bag-gml-in-rd gml3-as-jts as-etrs89 jts-as-wkt) (-> bag-gml-in-etrs89 gml3-in-etrs89-as-jts jts-as-wkt))))


  ; (def transformed-gml (transform (TransformXSLT. (clojure.java.io/input-stream (clojure.java.io/resource "pdok/featured/xslt/curve2linearring.xsl"))) (strip-gml-ns gml-surface)))
  ; (parse-to-jts transformed-gml)

