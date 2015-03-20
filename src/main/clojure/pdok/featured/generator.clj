(ns pdok.featured.generator
  (:require [cheshire.core :as json]
            [clj-time.format :as tf]
            [clj-time.local :refer [local-now]])
  (:import  [java.io PipedInputStream PipedOutputStream]))

;; 2015-02-26T15:48:26.578Z
(def ^{:private true} date-time-formatter (tf/formatters :date-time) )

(defn random-word [length]
  (let [chars (map char (range 97 123))
        word (take length (repeatedly #(rand-nth chars)))]
    (reduce str word)))

(defn random-geometry []
  {:type "gml" :gml "<gml:Surface srsName=\"urn:ogc:def:crs:EPSG::28992\"><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing>      <gml:posList srsDimension=\"2\" count=\"5\">000350.000 000650.000 000300.000 000650.000 000300.000 000600.000 000350.000 000600.000 000350.000 000650.000</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>"})


(defn random-new-feature
  ([collection] (random-new-feature collection (repeatedly 2 #(random-word 5))))
  ([collection other-fields] (random-new-feature collection (random-word 10) other-fields))
  ([collection id other-fields]
   (let [validity (tf/unparse date-time-formatter (local-now))
         base {:_action "new"
              :_collection collection
              :_id id
              :_validity validity
               :_geometry (random-geometry)}
         random-values (map #(vector % (random-word 5)) other-fields)
         feature (reduce (fn [acc val] ( apply assoc acc val)) base random-values)]

     feature)))

(defn random-json-features [out-stream dataset collection total]
  (let [other-fields (repeatedly 2 #(random-word 5))
        package {:_meta {}
             :dataset dataset
             :features (repeatedly total #(random-new-feature collection other-fields))}]
    (json/generate-stream package out-stream)))

(defn- random-json-feature-stream* [out-stream dataset collection total]
  (with-open [writer (clojure.java.io/writer out-stream)]
    (random-json-features writer dataset collection total)))

(defn random-json-feature-stream [dataset collection total]
  (let [pipe-in (PipedInputStream.)
        pipe-out (PipedOutputStream. pipe-in)]
    (future (random-json-feature-stream* pipe-out dataset collection total))
  pipe-in))

(defn generate-test-files []
  (doseq [c [10]]
    (with-open [w (clojure.java.io/writer (str ".test-files/new-features-single-collection-" c ".json"))]
      (random-json-features w "testset" "collection1" c))))
