(ns pdok.featured.json-reader
  (:require [pdok.featured.feature :refer [nilled as-jts]]
            [cheshire [core :as json] [factory :as jfac] [parse :as jparse]]
            [clj-time [core :as t] [format :as tf] [coerce :as tc]]
            [clojure.walk :refer [postwalk]])
  (:import (com.fasterxml.jackson.core JsonFactory JsonFactory$Feature
                                       JsonParser$Feature JsonParser JsonToken)
           (pdok.featured GeometryAttribute)))

(def ^:private pdok-field-replacements
  {"_action" :action "_collection" :collection "_id" :id "_validity" :validity
   "_geometry" :geometry "_current_validity" :current-validity
   "_parent_id" :parent-id "_parent_collection" :parent-collection "_parent_field" :parent-field})

(declare parse-time
         parse-date
         geometry-from-json
         upgrade-data)

(defn map-to-feature [obj]
  (let [action (keyword (get obj "_action"))
        validity (parse-time (obj "_validity"))
        current-validity (parse-time (obj "_current_validity"))
        feature (cond-> (upgrade-data obj)
                    true (assoc :action action)
                    true (assoc :validity validity)
                    current-validity (assoc :current-validity current-validity))]
    feature))

;; 2015-02-26T15:48:26.578Z
(def ^{:private true} date-time-formatter (tf/with-zone (tf/formatters :date-time-parser) (t/default-time-zone)))

(def ^{:private true} date-formatter (tf/formatter "yyyy-MM-dd"))

(defn parse-time
  "Parses an ISO8601 date timestring to local date time"
  [datetimestring]
  (when-not (clojure.string/blank? datetimestring)
    (tc/to-local-date-time (tf/parse date-time-formatter datetimestring))))

(defn parse-date
  "Parses a date string to local date"
  [datestring]
  (when-not (clojure.string/blank? datestring)
    (tc/to-local-date (tf/parse date-formatter datestring))))

(defn clojurify [s]
  (keyword (cond->
               (clojure.string/replace s #"_" "-")
               (clojure.string/starts-with? s "_")
               (subs 1))))

(defn- parse-object [^JsonParser jp]
  (jparse/parse* jp identity nil nil))

(defn- read-meta-data [^JsonParser jp]
  (.nextToken jp)
  (loop [state {}
         currentName (-> jp .getCurrentName .toLowerCase)]
    (if (= "features" currentName)
      state
      (let [obj (do (.nextToken jp) (parse-object jp))
            newName (-> (doto jp .nextToken) .getCurrentName .toLowerCase)]
        (recur (merge state {(clojurify currentName) obj}) newName)))
    ))

(defn- read-features [^JsonParser jp]
  (if (and (.nextToken jp) (not= (.getCurrentToken jp) JsonToken/END_ARRAY))
    (lazy-seq (cons (assoc (parse-object jp) :src :json) (read-features jp)))
    [])
  )

(defn- features-from-stream* [^JsonParser jp & overrides]
  (.nextToken jp)
  (when (= JsonToken/START_OBJECT (.getCurrentToken jp))
    (let [meta (read-meta-data jp)]
      ;; features should be array
      (when (= JsonToken/START_ARRAY (.nextToken jp))
        [meta (map map-to-feature (read-features jp))])
        )))

(defn features-from-stream [input-stream & args]
  "Parses until 'features' for state. Then returns vector [meta <lazy sequence of features>]."
  (let [reader (clojure.java.io/reader input-stream)
        factory jfac/json-factory
        parser (.createParser factory reader)
        meta-and-features (apply features-from-stream* parser args)]
    meta-and-features))

(defn file-stream [path]
  (clojure.java.io/reader path))

(defn- element-is-function? [element]
  (and (vector? element)
       (= 2 (count element))
       (string? (first element))
       (-> element first (clojure.string/starts-with? "~#"))))

(defn- get-valid-srid [geometry]
  (if-let [srid (get geometry "srid")]
    (Integer. srid)))

(defn create-geometry-atrribute [geometry]
  (if-let [type (get geometry "type")]
    (GeometryAttribute. type (get geometry type) (get-valid-srid geometry))))

(defn- evaluate-f [element]
  (let [[function params] element]
    (case function
      "~#moment"  (if params (apply parse-time params) (nilled org.joda.time.DateTime))
      "~#date"    (if params (apply parse-date params) (nilled org.joda.time.LocalDate))
      "~#int"     (if params (int (first params)) (nilled java.lang.Integer))
      "~#boolean" (if params (boolean (first params)) (nilled java.lang.Boolean))
      "~#double"  (if params (double (first params)) (nilled java.lang.Double))
      "~#geometry" (if params (create-geometry-atrribute params) (nilled pdok.featured.GeometryAttribute))
      element ; never fail just return element
      ))
  )

(defn- element-is-pdok-field? [element]
  (contains? pdok-field-replacements element))

(defn- pdok-field [element]
  (get pdok-field-replacements element))

(defn- replace-fn [element]
  (condp #(%1 %2) element
    element-is-function? (evaluate-f element)
    element-is-pdok-field? (pdok-field element)
    ;; else just return element, replace nothing
    element))

(defn- upgrade-geometry [element]
  (if (:geometry element)
    (update element :geometry create-geometry-atrribute)
    element))

(defn- upgrade-data [attributes]
  "Replaces functions with their parsed values"
  (let [attributes (postwalk replace-fn attributes)]
    (postwalk upgrade-geometry attributes)))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
