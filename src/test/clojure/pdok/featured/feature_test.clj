(ns pdok.featured.feature-test
  (:require [clojure.test :refer :all]
            [pdok.featured.feature :refer :all]
            [clojure.java.io :refer :all])
  (:import [pdok.featured.xslt TransformXSLT]))


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
  
  ; (def transformed-gml (transform (TransformXSLT. (clojure.java.io/input-stream (clojure.java.io/resource "pdok/featured/xslt/curve2linearring.xsl"))) (strip-gml-ns gml-surface)))
  ; (parse-to-jts transformed-gml)
  
