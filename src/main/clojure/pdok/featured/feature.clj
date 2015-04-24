(ns pdok.featured.feature
  (:refer-clojure :exclude [type])
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as j]
            [pdok.postgres :as pg]
            [cognitect.transit :as transit])
  (:import [pdok.featured WstxParser]
           [org.geotools.gml2 SrsSyntax]
           [org.geotools.gml3 GMLConfiguration]
           [com.vividsolutions.jts.geom Geometry]
           [com.vividsolutions.jts.io WKTWriter]
           ))

(deftype NilAttribute [class]
  Object
  (toString [_] nil)
  clojure.lang.IEditableCollection
  (asTransient [this] this)
  clojure.lang.ITransientAssociative
  (conj [this _] this)
  (persistent [v] v)
  (assoc [this _ _] this)
  (valAt [_ _] class)
  (valAt [_ _ _] class)
  j/ISQLValue
  (sql-value [v] nil)
  clojure.lang.IMeta
  (meta [_] {:type class})
  )

(defn nilled [class]
  (->NilAttribute class))

(def nil-attribute-writer
  (transit/write-handler
   "x"
   (fn [v] (.getName (:class v)))
   (fn [v] (.getName (:class v)))))

(pg/register-transit-handler pdok.featured.feature.NilAttribute nil-attribute-writer)

(def gml3-configuration
  (doto (GMLConfiguration.) (.setSrsSyntax SrsSyntax/OGC_URN)))

(def gml3-parser
  ;"gml v3 parser, which requires that gml: namespace prefixes are removed"
  (doto (WstxParser. gml3-configuration)
    (.setValidating true)
    (.setFailOnValidationError true)))

(defn strip-gml-ns [input]
  (str/replace input "gml:" ""))

(defmulti as-gml (fn [obj] str/lower-case (get obj "type")))

(defmethod as-gml "gml" [obj] (get obj "gml"))
(defmethod as-gml nil [obj] nil)

(defmulti as-jts (fn [obj] str/lower-case (get obj "type")))
(defmethod as-jts nil [_] nil)
(defmethod as-jts "gml" [obj]
  (let [gml (-> obj (get "gml") strip-gml-ns (.getBytes "UTF-8"))
        in (io/input-stream gml)]
    (.parse gml3-parser in)))

(defmethod as-jts "jts" [obj]
  (get obj "jts"))

(defmulti as-wkt (fn [obj] str/lower-case (get obj "type")))

(def wkt-writer (WKTWriter.))

(defn jts-as-wkt [jts]
  (.write wkt-writer jts))

(defmethod as-wkt "gml" [obj]
  (let [jts (as-jts obj)
        wkt (jts-as-wkt jts)]
    wkt))
