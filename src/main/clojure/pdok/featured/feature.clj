(ns pdok.featured.feature
  (:refer-clojure :exclude [type])
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as j]
            [pdok.postgres :as pg]
            [cognitect.transit :as transit]
            [clojure.java.io :as io])
  (:import [nl.pdok.gml3 GML3Parser]
           [pdok.featured.xslt TransformXSLT]
           [com.vividsolutions.jts.geom Geometry]
           [com.vividsolutions.jts.io WKTWriter]))

(def lower-case
  (fnil str/lower-case ""))

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
  clojure.lang.Seqable
   (seq [_] nil)
  )

(defn nilled [class]
  (->NilAttribute class))

(def nil-attribute-writer
  (transit/write-handler
   "x"
   (fn [v] (.getName (:class v)))
   (fn [v] (.getName (:class v)))))

(def nil-attribute-reader
  (transit/read-handler
   (fn [v]
     (->NilAttribute (symbol v)))))

(pg/register-transit-write-handler pdok.featured.feature.NilAttribute nil-attribute-writer)
(pg/register-transit-read-handler "x" nil-attribute-reader)

(def xslt-simple-gml (io/resource "pdok/featured/xslt/imgeo2simple-gml.xsl"))

(def simple-gml-transfomer (TransformXSLT. (io/input-stream xslt-simple-gml)))

(def gml3-parser (GML3Parser.))

(defn gml3-as-jts [gml]
  (.toJTSGeometry gml3-parser gml))

(def wkt-writer (WKTWriter.))

(defn jts-as-wkt [jts]
  (.write wkt-writer jts))

(defmulti as-gml (fn [obj] lower-case (get obj "type")))
(defmethod as-gml "gml" [obj] (get obj "gml"))
(defmethod as-gml :default [obj] nil)

(defmulti as-jts (fn [obj] lower-case (get obj "type")))
(defmethod as-jts :default [_] nil)
(defmethod as-jts "gml" [obj]
 (when-let [gml (get obj "gml")]
  (gml3-as-jts gml)))
(defmethod as-jts "jts" [obj]
  (get obj "jts"))

(defmulti as-simple-gml (fn [obj] lower-case (get obj "type")))
(defmethod as-simple-gml "gml" [obj] 
  (when-let [gml (get obj "gml")]
    (.transform simple-gml-transfomer gml)))
(defmethod as-simple-gml :default [obj] nil)

(defmulti as-wkt (fn [obj] lower-case (get obj "type")))
(defmethod as-wkt "gml" [obj]
  (let [jts (as-jts obj)
        wkt (jts-as-wkt jts)]
    wkt))

(defmulti geometry-group
  "returns :point, :line or :polygon"
  (fn [obj] (lower-case (get obj "type"))))

(defmethod geometry-group :default [_] nil)

(defn- starts-with-get [against-set test-value]
  (some (fn [t] (.startsWith test-value t)) against-set))

(defn- geometry-group*
  ([point-types line-types test-value]
   (geometry-group* point-types line-types test-value get))
  ([point-types line-types test-value predicate]
    (condp predicate test-value
      point-types :point
      line-types :line
      :polygon)))

;; http://schemas.opengis.net/gml/3.1.1/base/geometryPrimitives.xsd

(def gml-point-types
  #{"Point" "MultiPoint"})

;; TODO alles toevoegen, of starts with
(def gml-line-types
  #{"Curve" "CompositeCurve" "Arc" "ArcString" "Circle" "LineString"})

(defmethod geometry-group "gml" [obj]
  (let [re-result (re-find #"^<(gml:)?([^\s]+)" (get obj "gml"))
        type (when re-result (nth re-result 2))]
    (geometry-group* gml-point-types gml-line-types type)))

(def jts-point-types
  "Geometry types of Point-category"
  #{"Point" "MultiPoint"})

(def jts-line-types
  "Geometry types of Line-category"
  #{"Line" "LineString" "MultiLine"})

(defmethod geometry-group "jts" [obj]
  (let [type (-> obj (get "jts") .getGeometryType)]
    (geometry-group* jts-point-types jts-line-types type)))
