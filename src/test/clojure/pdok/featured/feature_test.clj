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

(deftest test-as-jts-with-curves
 (is (re-find #"curve" (as-gml gml-object-with-hole)))
 (is (not (re-find #"curve" (as-gml gml-object-without-curves))))
 (is (= (as-jts gml-object-with-hole) (as-jts gml-object-without-curves))))

(deftest test-gml-one-curve
  (let [transformed-gml (-> gml-one-curve strip-gml-ns transform-curves)]
  (is (re-find #"<LineStringSegment" transformed-gml))
  (is (re-find #"</LineStringSegment>" transformed-gml))
  (is (re-find #"076492.094 453702.905 076597.300 453750.312 076599.693 453751.441 076602.252 453752.711 076605.353 453754.372 076608.285 453756.106 076611.245 453758.008 076614.154 453759.983 076628.035 453769.741 076630.984 453771.756 076633.898 453773.622 076636.980 453775.425 076640.113 453777.138 076669.505 453792.674 076765.562 453843.434" transformed-gml))))

(deftest test-gml-surface 
  (let [transformed-gml (-> gml-surface strip-gml-ns transform-curves)]
    (is (re-find #"<Polygon" transformed-gml))
    (is (re-find #"196305.082 391991.976" transformed-gml))
    (is (re-find #"196225.502 391965.932" transformed-gml))
    (is (re-find #"196224.480 391964.440" transformed-gml))))

(deftest test-gml-with-more-elements
  (let [transformed-gml (-> gml-surface-with-more-elements strip-gml-ns transform-curves)]
  (is (= "MULTIPOLYGON (((176567.478 317267.125, 176564.507 317270.393, 176565.536 317274.689, 176565.536 317274.689, 176566.605 317276.087, 176566.605 317276.087, 176555.148 317284.886, 176555.148 317284.886, 176546.995 317291.218, 176546.853 317291.034, 176546.853 317291.034, 176545.616 317291.884, 176545.616 317291.884, 176538.072 317297.739, 176538.072 317297.739, 176536.28 317299.2, 176534.873 317297.386, 176534.873 317297.386, 176536.046 317296.508, 176536.567 317296.119, 176536.567 317296.119, 176529.917 317287.232, 176529.917 317287.232, 176522.866 317278.148, 176524.501 317276.878, 176519.993 317271.073, 176525.917 317266.473, 176525.917 317266.473, 176528.32 317264.612, 176533.311 317260.321, 176533.311 317260.321, 176531.591 317258.301, 176549.711 317240.064, 176549.711 317240.064, 176554.376 317235.364, 176555.206 317236.866, 176555.206 317236.866, 176557.689 317241.36, 176556.843 317241.223, 176556.843 317241.223, 176556.401 317241.151, 176556.401 317241.151, 176561.769 317255.571, 176565.845 317264.483, 176567.478 317267.125)))" 
       (.toString (parse-to-jts transformed-gml))))))
  
  ; (def transformed-gml (transform (TransformXSLT. (clojure.java.io/input-stream (clojure.java.io/resource "pdok/featured/xslt/curve2linearring.xsl"))) (strip-gml-ns gml-surface)))
  ; (parse-to-jts transformed-gml)
  
