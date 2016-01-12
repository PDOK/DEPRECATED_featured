(ns ^:regression pdok.featured.regression-test
  (:require [pdok.featured
             [dynamic-config :as dc]
             [config :as config]
             [persistence :as persistence]
             [geoserver :as geoserver]
             [timeline :as timeline]
             [processor :as processor]
             [json-reader :as json-reader]]
            [clojure.java.jdbc :as j]
            [clojure.test :refer :all]))

(def test-db config/processor-db)

(def test-gml
    "<gml:Point srsName=\"urn:ogc:def:crs:EPSG::28992\" xmlns:gml=\"http://www.opengis.net/gml\"><gmp:pos>172307.599 509279.740</gml:pos></gml:Point>"
 )

(defmacro defregressiontest [name & body]
  `(deftest ~name
     (with-bindings
       {#'dc/*persistence-schema* :featured_regression
        #'dc/*timeline-schema* :featured_regression}
       ~@body)))

(defn db-fixture [test]
  ;; setup
  (test)
  ;; tear down
  (j/execute! test-db ["DROP SCHEMA IF EXISTS featured_regression CASCADE"])
  (j/execute! test-db ["DROP SCHEMA IF EXISTS \"regression-set\" CASCADE"])
  )

(use-fixtures :each db-fixture)

(defn process-resource [file-name]
  (let [stream (clojure.java.io/resource file-name)
        [meta features] (json-reader/features-from-stream stream :dataset "regression-set")
        persistence (config/persistence)
        projectors (conj (config/projectors persistence) (config/timeline persistence))
        processor (processor/create meta persistence projectors)]
    (dorun (processor/consume processor features))
    (processor/shutdown processor)))

(defn- query [table collection feature-id]
  (j/query test-db [ (str "SELECT * FROM featured_regression." table " WHERE dataset = 'regression-set' and collection = ? and feature_id = ?") collection feature-id]))

(defn- test-persistence [collection feature-id {:keys [events features]}]
    (is (= events (count (query "feature_stream" collection feature-id))))
    (is (= features (count (query "feature" collection feature-id)))))

(defn- test-timeline [collection feature-id {:keys [timeline-current timeline]}]
    (is (= timeline-current (count (query "timeline_current" collection feature-id))))
    (is (= timeline (count (query "timeline" collection feature-id)))))


;(defregressiontest new-feature
;  (let [stats (:statistics (process-resource "regression/new-feature.json"))]
;    (is (= 1 (:n-processed stats)))
;    (is (= 0 (:n-errored stats)))))

(defregressiontest new-feature
  (let [stats (:statistics (process-resource "regression/col-2_id-b_new.json"))]
    (is (= 1 (:n-processed stats)))
    (is (= 0 (:n-errored stats)))
    (test-persistence "col-2" "id-b" {:events 1 :features 1})
    (test-timeline "col-2" "id-b" {:timeline-current 1 :timeline 0})))

(defregressiontest new-change-feature
  (let [stats (:statistics (process-resource "regression/col-2_id-b_new-change.json"))]
    (is (= 2 (:n-processed stats)))
    (is (= 0 (:n-errored stats)))
    (test-persistence "col-2" "id-b" {:events 2 :features 1})
    (test-timeline "col-2" "id-b" {:timeline-current 1 :timeline 1})))

(defregressiontest new-change-close-feature
  (let [stats (:statistics (process-resource "regression/col-2_id-b_new-change-close.json"))]
    (is (= 3 (:n-processed stats)))
    (is (= 0 (:n-errored stats)))
    (test-persistence "col-2" "id-b" {:events 3 :features 1})
    (test-timeline "col-2" "id-b" {:timeline-current 1 :timeline 1})))

(defregressiontest new-change-close_with_attributes-feature
  (let [stats (:statistics (process-resource "regression/col-2_id-b_new-change-close_with_attributes.json"))]
    (is (= 4 (:n-processed stats)))
    (is (= 0 (:n-errored stats)))
    (test-persistence "col-2" "id-b" {:events 4 :features 1})
    (test-timeline "col-2" "id-b" {:timeline-current 1 :timeline 2})))



