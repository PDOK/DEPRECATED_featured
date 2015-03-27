(ns pdok.featured.generator
  (:require [cheshire.core :as json]
            [clj-time [format :as tf]
             [local :refer [local-now]]])
  (:import  [java.io PipedInputStream PipedOutputStream]))

;; 2015-02-26T15:48:26.578Z
(def ^{:private true} date-time-formatter (tf/formatters :date-time) )

(def ^{:private true} date-formatter (tf/formatters :date) )

(defn random-word [length]
  (let [chars (map char (range 97 123))
        word (take length (repeatedly #(rand-nth chars)))]
    (reduce str word)))

(defn random-geometry []
  {:type "gml" :gml "<gml:Surface srsName=\"urn:ogc:def:crs:EPSG::28992\"><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing>      <gml:posList srsDimension=\"2\" count=\"5\">000350.000 000650.000 000300.000 000650.000 000300.000 000600.000 000350.000 000600.000 000350.000 000650.000</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>"})

(defn random-date []
  (let [date (local-now)
        date-string (tf/unparse date-formatter date)]
    ["~#date", [date-string]]))

(defn random-moment []
  (let [moment (local-now)
        date-time-string (tf/unparse date-time-formatter moment)]
    ["~#moment", [date-time-string]]))

(def attribute-generators [#(random-word 5) random-date random-moment])

(defn random-new-feature
  ([collection id validity attributes]
   (let [base {:_action "new"
              :_collection collection
              :_id id
              :_validity validity
               :_geometry (random-geometry)}
         random-values (map (fn [[key gen]] (vector key (gen))) attributes)
         feature (reduce (fn [acc val] ( apply assoc acc val)) base random-values)]

     feature)))

(defn update-an-attribute [attributes update-fn exceptions]
  (let [valid-keys (keys (apply dissoc attributes exceptions))
        key (rand-nth valid-keys)]
    (update-fn attributes key)))

(defn remove-an-attribute [attributes]
  (update-an-attribute attributes #(dissoc %1 %2)
                       [:_action :_collection :_id :_validity :_current_validity]))

(defn nillify-an-attribute [attributes]
  (update-an-attribute attributes #(assoc %1 %2 nil)
                       [:_action :_collection :_id :_validity :_current_validity :_geometry]))

(defn random-change-feature
  ([collection id current-validity attributes]
   (let [validity (tf/unparse date-time-formatter (local-now))
         base {:_action "change"
               :_collection collection
               :_id id
               :_current_validity current-validity
               :_validity validity
               :_geometry (random-geometry)}
         random-values (map (fn [[key gen]] (vector key (gen))) attributes)
         feature (-> (reduce (fn [acc val] ( apply assoc acc val)) base random-values)
                     remove-an-attribute
                     nillify-an-attribute
                     )
         ]

     feature)))

(defn random-json-features [out-stream dataset collection total with-update]
  (let [id (random-word 10)
        validity (tf/unparse date-time-formatter (local-now))
        other-fields (repeatedly 3 #(random-word 5))
        attributes (map #(vector %1 %2) other-fields attribute-generators)
        gens (transient [])
        gens (conj! gens (fn [id] ( random-new-feature collection id validity attributes)))
        gens (if with-update (conj! gens (fn [id] (random-change-feature collection id validity attributes))) gens)
        generator (apply juxt (persistent! gens))
        gen-fn #(generator (random-word 10))
        package {:_meta {}
                 :dataset dataset
                 :features (mapcat identity (repeatedly total gen-fn))}]
    (json/generate-stream package out-stream)))

(defn- random-json-feature-stream* [out-stream dataset collection total with-update]
  (with-open [writer (clojure.java.io/writer out-stream)]
    (random-json-features writer dataset collection total with-update)))

(defn random-json-feature-stream
  ([dataset collection total]
   (random-json-feature-stream dataset collection total false))
  ([dataset collection total with-update]
   (let [pipe-in (PipedInputStream.)
         pipe-out (PipedOutputStream. pipe-in)]
     (future (random-json-feature-stream* pipe-out dataset collection total with-update))
     pipe-in)))

(defn generate-test-files []
  (doseq [c [10 100 1000 10000 100000]]
    (with-open [w (clojure.java.io/writer (str ".test-files/new-features-single-collection-" c ".json"))]
      (random-json-features w "newset" "collection1" c false))))

(defn generate-test-files-with-updates []
  (doseq [c [5 50 500 5000 50000]]
    (with-open [w (clojure.java.io/writer (str ".test-files/update-features-single-collection-" c ".json"))]
      (random-json-features w "updateset" "collection1" c true))))
