(ns pdok.featured.feature
  (:refer-clojure :exclude [type])
  (:require [clojure.string :as str]
            [clojure.java.io :as io])
  (:import [pdok.featured WstxParser]
           [org.geotools.gml2 SrsSyntax]
           [org.geotools.gml3 GMLConfiguration]
           [com.vividsolutions.jts.geom Geometry]
           [com.vividsolutions.jts.io WKTWriter]
           ))

(def gml3-configuration
  (doto (GMLConfiguration.) (.setSrsSyntax SrsSyntax/OGC_URN)))

(def gml3-parser
  ;"gml v3 parser, which requires that gml: namespace prefixes are removed"
  (doto (WstxParser. gml3-configuration)
    (.setValidating true)
    (.setFailOnValidationError true)))

(defn strip-gml-ns [input]
  (str/replace input "gml:" ""))

(defrecord NewFeature [dataset collection id validity geometry attributes])
(defrecord ChangeFeature [dataset collection id validity current-validity geometry attributes])
(defrecord CloseFeature [dataset collection id validity current-validity geometry attributes])
(defrecord DeleteFeature [dataset collection id current-validity])

(defmulti as-gml (fn [obj] str/lower-case (get obj "type")))

(defmethod as-gml "gml" [obj] (get obj "gml"))
(defmethod as-gml nil [obj] nil)

(defmulti as-jts (fn [obj] str/lower-case (get obj "type")))

(defmethod as-jts "gml" [obj]
  (let [gml (-> obj (get "gml") strip-gml-ns (.getBytes "UTF-8"))
        in (io/input-stream gml)]
    (.parse gml3-parser in)))

(defmulti as-wkt (fn [obj] str/lower-case (get obj "type")))

(def wkt-writer (WKTWriter.))

(defn jts-as-wkt [jts]
  (.write wkt-writer jts))

(defmethod as-wkt "gml" [obj]
  (let [jts (as-jts obj)
        wkt (jts-as-wkt jts)]
    wkt))
