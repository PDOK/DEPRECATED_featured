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

(defregressiontest new-feature
  (let [stats (:statistics (process-resource "regression/new-feature.json"))]
    (is (= 1 (:n-processed stats)))
    (is (= 0 (:n-errored stats)))))
