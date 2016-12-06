(ns pdok.featured.json-writer
  (:require [pdok.featured.projectors :as proj]
            [clojure.data.json :as json])
  (:import (pdok.featured NilAttribute)
           (org.joda.time LocalDate LocalDateTime)))

(defn- render-map-generic [data]
  (json/write-str data
                  :value-fn (fn [key value]
                              (if (instance? NilAttribute value)
                                nil ;; TODO: nil LocalDate(Time)s should still have ~#date and ~#moment.
                                (if (instance? LocalDate value)
                                  ["~#date" (str value)]
                                  (if (instance? LocalDateTime value)
                                    ["~#moment" (str value)]
                                    value))))))

(defn- write-feature [feature]
  (println
    (render-map-generic
      (-> {"_action" (:action feature)
           "_collection" (:collection feature)
           "_id" (:id feature)
           "_validity" (:validity feature)}
          (merge (if (not (nil? (:current-validity feature)))
                   {"_current_validity" (:current-validity feature)}
                   nil))
          (merge (if (not (nil? (:geometry feature)))
                   {"_geometry" (:geometry feature)}
                   nil))
          (merge (:attributes feature))))))

(defrecord JsonWriterProjector [dataset]
  proj/Projector
  (proj/init [this for-dataset current-collections]
    (println "Init")
    this)
  (proj/new-collection [this collection parent-collection]
    (println "New collection")
    this)
  (proj/flush [this]
    (println "Flush")
    this)
  (proj/new-feature [this feature]
    (write-feature feature)
    this)
  (proj/change-feature [this feature]
    (write-feature feature)
    this)
  (proj/close-feature [this feature]
    (write-feature feature)
    this)
  (proj/delete-feature [this feature]
    (write-feature feature)
    this)
  (proj/accept? [_ _]
    true)
  (proj/close [this]
    (println "Close")
    this))

(defn json-writer-projector [config]
  (let [dataset "unknown-dataset"]
    (->JsonWriterProjector dataset)))
