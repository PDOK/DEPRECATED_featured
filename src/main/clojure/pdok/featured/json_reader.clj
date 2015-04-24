(ns pdok.featured.json-reader
  (:require [pdok.featured.feature :refer [nilled]]
   [cheshire [core :as json] [factory :as jfac] [parse :as jparse]]
            [clj-time.format :as tf]
            [clojure.walk :refer [postwalk]])
  (:import (com.fasterxml.jackson.core JsonFactory JsonFactory$Feature
                                       JsonParser$Feature JsonParser JsonToken)))

(def ^:private pdok-field-replacements
  {"_action" :action "_collection" :collection "_id" :id "_validity" :validity
   "_geometry" :geometry "_current_validity" :current-validity})

(declare parse-time)
(declare geometry-from-json)
(declare upgrade-data)

(defn map-to-feature [dataset obj]
  (let [action (keyword (get obj "_action"))
        validity (parse-time (obj "_validity"))
        feature (-> obj upgrade-data
                    (assoc :dataset dataset)
                    (assoc :action action)
                    (assoc :validity validity))]
    feature))

;; 2015-02-26T15:48:26.578Z
(def ^{:private true} date-time-formatter (tf/formatters :date-time-parser) )

(defn- parse-time
  "Parses an ISO8601 date timestring to local date"
  [datetimestring]
  (tf/parse date-time-formatter datetimestring))

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

(defn- features-from-stream* [^JsonParser jp & {:keys [dataset]}]
  (.nextToken jp)
  (when (= JsonToken/START_OBJECT (.getCurrentToken jp))
    (let [meta (read-meta-data jp)
          dataset (or dataset (get meta "dataset"))]
      (if-not dataset
        (throw (Exception. "dataset needed"))
        ;; features should be array
        (when (= JsonToken/START_ARRAY (.nextToken jp))
          (map (partial map-to-feature dataset) (read-features jp)))
        ))))

(defn features-from-stream [input-stream & args]
  "Parses until 'features' for state. Then returns lazy sequence of features."
  (let [reader (clojure.java.io/reader input-stream)
        factory jfac/json-factory
        parser (.createParser factory reader)
        features (apply features-from-stream* parser args)]
    features))

(defn file-stream [path]
  (clojure.java.io/reader path))

(defn- element-is-function? [element]
  (and (vector? element)
       (= 2 (count element))
       (string? (first element))
       (-> element first (.startsWith "~#"))))

(defn- evaluate-f [element]
  (let [[function params] element]
      (case function
        "~#moment"  (if params (apply parse-time params) (nilled org.joda.time.DateTime))
        "~#date"    (if params (apply parse-time params) (nilled org.joda.time.DateTime))
        "~#int"     (if params (int (first params)) (nilled java.lang.Integer))
        "~#boolean" (if params (boolean (first params)) (nilled java.lang.Boolean))
        "~#double"  (if params (double (first params)) (nilled java.lang.Double))
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

(defn- upgrade-data [attributes]
  "Replaces functions with their parsed values"
  (postwalk replace-fn attributes)
  )

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
