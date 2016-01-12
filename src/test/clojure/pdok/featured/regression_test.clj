(ns ^:regression pdok.featured.regression-test
  (:require [pdok.featured
             [dynamic-config :as dc]
             [config :as config]
             [persistence :as persistence]
             [geoserver :as geoserver]
             [timeline :as timeline]
             [processor :as processor]
             [json-reader :as json-reader]]
            [clojure.math.combinatorics :as combo]
            [clojure.java.jdbc :as j]
            [clojure.test :refer :all]))

(def test-db config/processor-db)

(def test-gml
    "<gml:Point srsName=\"urn:ogc:def:crs:EPSG::28992\" xmlns:gml=\"http://www.opengis.net/gml\"><gmp:pos>172307.599 509279.740</gml:pos></gml:Point>"
 )

(defn with-drops [col]
  (drop 1 (reductions (fn [[d t] v] [(+ d t) v]) [0 0] col)))

(defn permutate-features [feature-stream]
  "Returns a sequence of collection with ordered subsets of the feature stream.
(abcd) wil become (((abcd))((abc)(d))((ab)(cd)) ... )"
  (let [features (doall feature-stream)
        singles (repeat (count features) 1)
        permutations (mapcat combo/permutations
                             (map (fn [c] (map #(count %) c)) (combo/partitions singles)))
        with-drops (map with-drops permutations)
        feature-sets (map (fn [p]
                            (map (fn [[d t] f] (take t (drop d f)))
                                 p (repeat features))) with-drops)]
    feature-sets))

(defn json-features [file-name]
  (let [stream (clojure.java.io/resource file-name)
        [meta features] (json-reader/features-from-stream stream :dataset "regression-set")]
    [meta features]))

(defn process-feature-permutation [meta feature-permutation]
  (apply merge-with (fn [a b] (if (seq? a) (conj a b) (+ a b)))
         (map (fn [features]
                (let [persistence (config/persistence)
                      projectors (conj (config/projectors persistence) (config/timeline persistence))
                      processor (processor/create meta persistence projectors)]
                  (dorun (processor/consume processor features))
                  (:statistics (processor/shutdown processor))))
              feature-permutation)))

(defn clean-db []
  (j/execute! test-db ["DROP SCHEMA IF EXISTS featured_regression CASCADE"])
  (j/execute! test-db ["DROP SCHEMA IF EXISTS \"regression-set\" CASCADE"]))

(defmacro defregressiontest [name file stats-var & body]
  `(deftest ~name
     (with-bindings
       {#'dc/*persistence-schema* :featured_regression
        #'dc/*timeline-schema* :featured_regression}
       (let [[meta# features#] (json-features (str "regression/" ~file ".json"))
             permutated# (permutate-features features#)]
         (doseq [p# permutated#]
           (clean-db)
           (let [~stats-var (process-feature-permutation meta# p#)]
             ~@body))))))

(defn- query [table selector]
  (let [clauses (timeline/map->where-clause selector)]
  (j/query test-db [ (str "SELECT * FROM featured_regression." table " "
                          "WHERE dataset = 'regression-set' "
                          "AND " clauses )])))

(defn- test-persistence [collection feature-id {:keys [events features]}]
    (is (= events (count (query "feature_stream" {:collection collection :feature_id feature-id}))))
    (is (= features (count (query "feature" {:collection collection :feature_id feature-id})))))


(defn- test-timeline [collection feature-id {:keys [timeline-current timeline]}]
    (is (= (:n timeline-current) (count (query "timeline_current" {:collection collection :feature_id feature-id}))))
    (is (= (:insert-in-delta timeline-current) (count (query "timeline_current_delta" {:collection collection :action "I"}))))
    (is (= (:delete-in-delta timeline-current) (count (query "timeline_current_delta" {:collection collection :action "D"}))))

    (is (= (:n timeline) (count (query "timeline" {:collection collection :feature_id feature-id}))))
    (is (= (:insert-in-delta timeline) (count (query "timeline_delta" {:collection collection :action "I"}))))
    (is (= (:delete-in-delta timeline) (count (query "timeline_delta" {:collection collection :action "D"}))))
    )


(defregressiontest new-feature "col-2_id-b_new" stats
  (is (= 1 (:n-processed stats)))
  (is (= 0 (:n-errored stats)))
  (test-persistence "col-2" "id-b" {:events 1 :features 1})
  (test-timeline "col-2" "id-b" {:timeline-current {:n 1 :delete-in-delta 0 :insert-in-delta 1}
                                 :timeline {:n 0 :delete-in-delta 0 :insert-in-delta 0}}))

(defregressiontest new-change-feature "col-2_id-b_new-change" stats
  (is (= 2 (:n-processed stats)))
  (is (= 0 (:n-errored stats)))
  (test-persistence "col-2" "id-b" {:events 2 :features 1})
  (test-timeline "col-2" "id-b" {:timeline-current {:n 1 :delete-in-delta 1 :insert-in-delta 2}
                                 :timeline {:n 1 :delete-in-delta 0 :insert-in-delta 1}}))

(defregressiontest new-change-close-feature "col-2_id-b_new-change-close" stats
  (is (= 3 (:n-processed stats)))
  (is (= 0 (:n-errored stats)))
  (test-persistence "col-2" "id-b" {:events 3 :features 1})
  (test-timeline "col-2" "id-b" {:timeline-current {:n 1 :delete-in-delta 2 :insert-in-delta 3}
                                 :timeline {:n 1 :delete-in-delta 0 :insert-in-delta 1}}))

(defregressiontest new-change-close_with_attributes-feature "col-2_id-b_new-change-close_with_attributes" stats
  (is (= 4 (:n-processed stats)))
  (is (= 0 (:n-errored stats)))
  (test-persistence "col-2" "id-b" {:events 4 :features 1})
  (test-timeline "col-2" "id-b" {:timeline-current {:n 1 :delete-in-delta 3 :insert-in-delta 4}
                                 :timeline {:n 2 :delete-in-delta 0 :insert-in-delta 2}}))

(defregressiontest new-change-change-delete-feature "col-1_id-a_new-change-change-delete" stats
  (is (= 4 (:n-processed stats)))
  (is (= 0 (:n-errored stats)))
  (test-persistence "col-1" "id-a" {:events 4 :features 1})
  (test-timeline "col-1" "id-a" {:timeline-current {:n 0 :delete-in-delta 3 :insert-in-delta 3}
                                 :timeline {:n 0 :delete-in-delta 3 :insert-in-delta 2}}))
