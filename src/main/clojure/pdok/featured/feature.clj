(ns pdok.featured.feature
  (:refer-clojure :exclude [type])
  (:require [clojure.string :as str]
            [clojure.core.cache :as cache]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as j]
            [pdok.postgres :as pg]
            [cognitect.transit :as transit]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log])
  (:import [nl.pdok.gml3.impl GMLMultiVersionParserImpl]
           [pdok.featured NilAttribute]
           [pdok.featured.xslt TransformXSLT]
           [pdok.featured.converters Transformer]
           [com.vividsolutions.jts.geom Geometry]
           [com.vividsolutions.jts.io WKTWriter WKTReader]
           (org.joda.time LocalDateTime)))

(def lower-case
  (fnil str/lower-case ""))

(defn nilled [clazz]
  (NilAttribute. clazz))

(def xslt-simple-gml (io/resource "pdok/featured/xslt/imgeo2simple-gml.xsl"))

(def simple-gml-transfomer (TransformXSLT. (io/input-stream xslt-simple-gml)))

(def gml3-parser
  (GMLMultiVersionParserImpl.))

(defn gml3-as-jts [gml]
  (try
    (.toJTSGeometry ^GMLMultiVersionParserImpl gml3-parser ^String gml)
    (catch nl.pdok.gml3.exceptions.GML3ParseException e
      (log/error "Could not transform GML to JTS:" (.getMessage  e))
      nil)))

(def wkt-writer (WKTWriter.))
(def wkt-reader (WKTReader.))

(defn jts-as-wkt [jts]
  (if-not (nil? jts)
    (.write ^WKTWriter wkt-writer jts)))

(defmulti valid-geometry? (fn [obj] (lower-case (get obj "type"))))
(defmethod valid-geometry? :default [_] nil)
(defmethod valid-geometry? "gml" [obj]
  (if (get obj "gml") true false))
(defmethod valid-geometry? "wkt" [obj]
  (if (get obj "wkt") true false))

(defmethod valid-geometry? "jts" [obj]
  true)

(defmulti as-gml (fn [obj] (lower-case (get obj "type"))))
(defmethod as-gml "gml" [obj] (when-let [gml (get obj "gml")]
                                (str/trim (reduce str (str/split-lines (str/trim-newline (str/replace gml #"<\?[^\?]*\?>" "")))))))
(defmethod as-gml :default [obj] nil)

(defmulti as-jts (fn [obj] (lower-case (get obj "type"))))
(defmethod as-jts :default [_] nil)

(def gml->jts-cache (atom (cache/lu-cache-factory {} :threshold 30000)))

(defmethod as-jts "gml" [obj]
 (when-let [gml (get obj "gml")]
   (if (cache/has? @gml->jts-cache gml)
     (cache/lookup @gml->jts-cache gml)
     (let [jts (gml3-as-jts gml)
           _ (swap! gml->jts-cache #(cache/miss % gml jts))]
       jts))))

(defmethod as-jts "jts" [obj]
  (get obj "jts"))

(defmethod as-jts "wkt" [obj]
  (let [jts (.read ^WKTReader wkt-reader ^String (get obj "wkt"))]
    (when-let [srid (get obj "srid")]
      ; srid should be an integer, but it might be a string
      (doto jts (.setSRID (if (string? srid) (Integer/parseInt srid) srid))))
    jts))

(defn as-rd [^Geometry geometry]
  (when geometry
    (if (= 28992 (.getSRID geometry))
      geometry
      (.transform Transformer/ETRS89ToRD geometry))))

(defn as-etrs89 [^Geometry geometry]
  (when geometry
    (if (= 4258 (.getSRID geometry))
      geometry
      (.transform Transformer/RDToETRS89 geometry))))


(defmulti as-simple-gml (fn [obj] lower-case (get obj "type")))
(defmethod as-simple-gml "gml" [obj]
  (when-let [gml (get obj "gml")]
    (.transform ^TransformXSLT simple-gml-transfomer gml)))
(defmethod as-simple-gml :default [obj] nil)

(defmulti as-wkt (fn [obj] lower-case (get obj "type")))
(defmethod as-wkt "gml" [obj]
  (let [jts (as-jts obj)
        wkt (jts-as-wkt jts)]
    wkt))

(defmulti geometry-group
  "returns :point, :line or :polygon"
  (fn [obj] (when obj
              (lower-case (get obj "type")))))

(defmethod geometry-group :default [_] nil)

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
  (when-let [gml-str (get obj "gml")]
    (let [re-result (re-find #"^(<\?[^\?]*\?>)?<([a-zA-Z0-9]+:)?([^\s]+)" gml-str)
          type (when re-result (nth re-result 3))]
      (geometry-group* gml-point-types gml-line-types type))))

(def jts-point-types
  "Geometry types of Point-category"
  #{"Point" "MultiPoint"})

(def jts-line-types
  "Geometry types of Line-category"
  #{"Line" "LineString" "MultiLine"})

(defmethod geometry-group "jts" [obj]
  (let [type (.getGeometryType ^Geometry (get obj "jts"))]
    (geometry-group* jts-point-types jts-line-types type)))

(defmethod geometry-group "wkt" [obj]
  (let [jts (as-jts obj)
        type (.getGeometryType ^Geometry jts)]
    (geometry-group* jts-point-types jts-line-types type)))

(defmulti as-stufgeo-field clojure.core/type)
(defmethod as-stufgeo-field LocalDateTime [tijdstipregistratie]
  (.toString tijdstipregistratie "yyyyMMddHHmmss"))
(defmethod as-stufgeo-field :default [_] nil)

(defn- map-replace [content & replacements]
  (let [replacement-list (partition 2 replacements)]
    (reduce #(apply str/replace %1 %2) content replacement-list)))

(defmulti as-stufgeo-gml (fn [obj] (lower-case (get obj "type"))))
(defmethod as-stufgeo-gml "gml" [obj]
  (when (get obj "gml")
    (map-replace (as-gml obj) #"(<gml:Curve .+</gml:Curve>)" "<imgeo:lijn>$1</imgeo:lijn>"
                 #"(<gml:LineString .+</gml:LineString>)" "<imgeo:lijn>$1</imgeo:lijn>"
                 #"(<gml:Surface .+</gml:Surface>)" "<imgeo:vlak>$1</imgeo:vlak>"
                 #"(<gml:MultiSurface .+</gml:MultiSurface>)" "<imgeo:multiVlak>$1</imgeo:multiVlak>"
                 #"(<gml:Point .+</gml:Point>)" "<imgeo:punt>$1</imgeo:punt>"
                 #"(<gml:MultiPoint .+</gml:MultiPoint>)" "<imgeo:multiPunt>$1</imgeo:multiPunt>"
                 #"<gml:Polygon (.+?)>(.+)</gml:Polygon>"
                 "<imgeo:vlak><gml:Surface $1><gml:patches><gml:PolygonPatch>$2</gml:PolygonPatch></gml:patches></gml:Surface></imgeo:vlak>")))
(defmethod as-stufgeo-gml :default [_] nil)

(defmulti as-stufgeo-gml-lc (fn [obj] (lower-case (get obj "type"))))
(defmethod as-stufgeo-gml-lc "gml" [obj]
  (when (get obj "gml")
    (map-replace (as-stufgeo-gml obj) #"(<imgeo:multiVlak>.+</imgeo:multiVlak>)" "<imgeo:multivlak>$1</imgeo:multivlak>"
                 #"(<imgeo:multiPunt>.+</imgeo:multiPunt>)" "<imgeo:multipunt>$1</imgeo:multipunt>")))
(defmethod as-stufgeo-gml-lc :default [_] nil)
