(ns ^:performance pdok.featured.performance-test
    (:require [pdok.featured
               [dynamic-config :as dc]
               [config :as config]
               [persistence :as persistence]
               [geoserver :as geoserver]
               [timeline :as timeline]
               [processor :as processor]
               [generator :as generator]
               [json-reader :as jr]]
              [pdok.postgres :as pg]
              [clojure.java.jdbc :as j]
              [clojure.test :refer :all])
    (:import [java.io ByteArrayInputStream]))

(def test-db config/processor-db)

(defn clean-db []
  (j/execute! test-db ["DROP SCHEMA IF EXISTS featured_performance CASCADE"])
  (j/execute! test-db ["DROP SCHEMA IF EXISTS \"performance-set\" CASCADE"]))

(defn generate-new-change-features [ids]
  (generator/random-json-feature-stream "performance-set" "col1" (* 2 (count ids))
                                        :ids ids :change? true))

(defn generate-delete-new-change-close-features [ids]
  (generator/random-json-feature-stream "performance-set" "col1" (* 4 (count ids))
                                        :ids ids :start-with-delete? true
                                        :change? true :close? true))

(defn run [feature-stream]
  (with-bindings
    {#'dc/*persistence-schema* :featured_performance
     #'dc/*timeline-schema* :featured_performance}
    (let [[meta features] (jr/features-from-stream (clojure.java.io/reader feature-stream))
          persistence (config/persistence)
          projectors (conj (config/projectors persistence) (config/timeline persistence))
          processor (processor/create {:check-validity-on-delete false} persistence projectors)]
      (dorun (processor/consume processor features))
      (:statistics (processor/shutdown processor)))))

(deftest double-file-test
  (clean-db)
  (doseq [i (range 1 6)]
    (let [n 5000
          ids (map str (range (* i n) (* (inc i) n)))
          stream-1 (.getBytes (slurp (generate-new-change-features ids)))
          stream-2 (.getBytes (slurp (generate-delete-new-change-close-features ids)))]
      (println "Run" i)
      (time (do (run (ByteArrayInputStream. stream-1))
                (run (ByteArrayInputStream. stream-2)))))))
