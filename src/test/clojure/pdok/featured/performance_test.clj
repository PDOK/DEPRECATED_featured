(ns ^:performance pdok.featured.performance-test
  (:require [pdok.featured
             [dynamic-config :as dc]
             [config :as config]
             [processor :as processor]
             [generator :as generator]
             [json-reader :as jr]]
            [clojure.java.jdbc :as j]
            [clojure.test :refer :all])
  (:import [java.io ByteArrayInputStream]))

(def test-db config/processor-db)

(defn clean-db []
  (j/execute! test-db ["DROP SCHEMA IF EXISTS \"featured_performance_performance-set\" CASCADE"])
  (j/execute! test-db ["DROP SCHEMA IF EXISTS \"performance-set\" CASCADE"]))

(defn vacuum-tables []
  (j/execute! test-db ["VACUUM ANALYZE \"featured_performance_performance-set\".feature"] :transaction? false)
  (j/execute! test-db ["VACUUM ANALYZE \"featured_performance_performance-set\".feature_stream"] :transaction? false)
  (j/execute! test-db ["VACUUM ANALYZE \"featured_performance_performance-set\".timeline_col1"] :transaction? false))

(defn generate-new-features [ids difficult?]
  (with-bindings {#'generator/*difficult-geometry?* difficult?}
    (generator/random-json-feature-stream "performance-set" "col1" (count ids)
                                          :ids ids)))

(defn generate-new-change-features [ids difficult?]
  (with-bindings {#'generator/*difficult-geometry?* difficult?}
    (generator/random-json-feature-stream "performance-set" "col1" (* 2 (count ids))
                                          :ids ids :change? true)))

(defn generate-delete-new-change-close-features [ids difficult?]
  (with-bindings {#'generator/*difficult-geometry?* difficult?}
    (generator/random-json-feature-stream "performance-set" "col1" (* 4 (count ids))
                                          :ids ids :start-with-delete? true
                                          :change? true :close? true)))

(defn generate-nested-new-change-features [ids difficult?]
  (with-bindings {#'generator/*difficult-geometry?* difficult?}
    (generator/random-json-feature-stream "performance-set" "col1" (* 3 (count ids))
                                          :ids ids :nested 2 :change? true)))

(defn run [cfg feature-stream]
  (with-bindings
    {#'dc/*persistence-schema-prefix* :featured_performance
     #'dc/*timeline-schema-prefix* :featured_performance}
    (let [[meta features] (jr/features-from-stream (clojure.java.io/reader feature-stream))
          persistence (config/persistence)
          timeline (config/timeline)
          processor (processor/create
                      (merge {:check-validity-on-delete false} cfg)
                      "performance-set" persistence [timeline])]
      (dorun (processor/consume processor features))
      (let [statistics (processor/shutdown processor)
            _ (vacuum-tables)]
      (:statistics statistics)))))

(deftest double-file-test
  (clean-db)
  (doseq [i (range 1 20)]
    (let [n 5000
          ids (map str (range (* i n) (* (inc i) n)))
          stream-1 (.getBytes (slurp (generate-new-change-features ids true)))
          stream-2 (.getBytes (slurp (generate-delete-new-change-close-features ids true)))]
      (println "Run" i)
      (time (do (run {} (ByteArrayInputStream. stream-1))
                (run {} (ByteArrayInputStream. stream-2)))))))

(deftest only-new-test
  (clean-db)
  (doseq [i (range 1 20)]
    (let [n 15000
          ids (map str (range (* i n) (* (inc i) n)))
          stream-1 (.getBytes (slurp (generate-new-features ids false)))]
      (println "Run" i)
      (time (run {:disable-validation true} (ByteArrayInputStream. stream-1))))))

(deftest nested-new-change-test
  (clean-db)
  (doseq [i (range 1 20)]
    (let [n 7500
          ids (map str (range (* i n) (* (inc i) n)))
          stream-1 (.getBytes (slurp (generate-nested-new-change-features ids false)))]
      (println "Run" i)
      (time (run {:disable-validation true} (ByteArrayInputStream. stream-1))))))
