(ns pdok.featured.json-reader
  (:require [pdok.featured.feature :refer [->NewFeature]]
            [cheshire [core :as json] [factory :as jfac] [parse :as jparse]]
            [clj-time.format :as tf]
            [clojure.walk :refer [postwalk]])
  (:import (com.fasterxml.jackson.core JsonFactory JsonFactory$Feature
                                       JsonParser$Feature JsonParser JsonToken)))

(def ^:private pdok-fields ["_action" "_collection" "_id" "_validity" "_geometry"])

(declare parse-time)
(declare attributes)
(declare geometry-from-json)
(declare parse-functions)

(defmulti map-to-feature (fn [dataset obj] (get obj "_action")))

(defmethod map-to-feature "new" [dataset obj]
  (let [collection (get obj "_collection")
        id (get obj "_id")
        validity (parse-time (get obj "_validity"))
        geom (get obj "_geometry")
        attributes (parse-functions (attributes obj))]
    (->NewFeature dataset collection id validity geom attributes)))

;; 2015-02-26T15:48:26.578Z
(def ^{:private true} date-time-formatter (tf/formatters :date-time-parser) )

(defn- parse-time
  "Parses an ISO8601 date timestring to local date"
  [datetimestring]
  (tf/parse date-time-formatter datetimestring))

(defn- attributes [obj]
  (apply dissoc obj pdok-fields)
  )

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
        (recur (merge state {currentName obj}) newName)))
    ))

(defn- read-features [^JsonParser jp]
  (if (and (.nextToken jp) (not= (.getCurrentToken jp) JsonToken/END_ARRAY))
    (lazy-seq (cons (parse-object jp) (read-features jp)))
    [])
  )

(defn- features-from-stream* [^JsonParser jp]
  (.nextToken jp)
  (when (= JsonToken/START_OBJECT (.getCurrentToken jp))
    (let [meta (read-meta-data jp)
          dataset (get meta "dataset")]
      (if-not dataset
        (throw (Exception. "dataset needed"))
        ;; features should be array
        (when (= JsonToken/START_ARRAY (.nextToken jp))
          (map (partial map-to-feature dataset) (read-features jp)))
        ))))

(defn features-from-stream [input-stream]
  "Parses until 'features' for state. Then returns lazy sequence of features."
  (let [reader (clojure.java.io/reader input-stream)
        factory jfac/json-factory
        parser (.createParser factory reader)
        features (features-from-stream* parser)]
    features))

(defn file-stream [path]
  (clojure.java.io/reader path))

(defn- element-is-function? [element]
  (and (vector? element)
           (= 2 (count element))
           (-> element first (.startsWith "~#"))))

(defn- replacer [element]
  (if (element-is-function? element)
    (let [[function params] element]
      (case function
        "~#moment" (apply parse-time params)
        "~#date"   (apply parse-time params)
        element ; never fail just return element
        ))
    ;else just return element, replace nothing
    element))

(defn- parse-functions [attributes]
  "Replaces functions with their parsed values"
  (postwalk replacer attributes)
  )

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
