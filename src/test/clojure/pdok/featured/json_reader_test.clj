(ns pdok.featured.json-reader-test
  (:require [pdok.featured.json-reader :refer :all]
            [pdok.util :as util]
            [clj-time.core :as time]
            [clojure.test :refer :all])
  (:import (pdok.featured GeometryAttribute)))

(deftest date-time-with-timezone-to-local
  (let [datetimestring "2016-01-01T12:00:00.000+01:00"
        required (time/local-date-time 2016 1 1 12 0 0 0)
        parsed (util/parse-time datetimestring)]
    (is (= required parsed))))

(deftest date-time-with-z-to-local
  (let [datetimestring "2016-01-01T12:00:00.000Z"
        required (time/local-date-time 2016 1 1 13 0 0 0)
        parsed (util/parse-time datetimestring)]
    (is (= required parsed))))

(deftest date-time-without-timezone-to-local
  (let [datetimestring "2016-01-01T12:00:00.000"
        required (time/local-date-time 2016 1 1 12 0 0 0)
        parsed (util/parse-time datetimestring)]
    (is (= required parsed))))

(def ^{:private true} wkt-multipolygon
  "MULTIPOLYGON (((114326.97399999946 488016.4820000008, 114317.15399999917 488016.37999999896, 114316.77800000086 488052.61300000176, 114326.59800000116 488052.7170000002, 114326.97399999946 488016.4820000008)), ((114326.44099999964 488067.7740000002, 114316.63500000164 488067.6700000018, 114316.24399999902 488103.8830000013, 114326.06599999964 488103.9849999994, 114326.44099999964 488067.7740000002)))")

(deftest srid-geometry-atrribute
  (let [geometry-map {"srid" 28992 "wkt" wkt-multipolygon "type" "wkt"}
        ^GeometryAttribute ga (create-geometry-attribute geometry-map)]
    (is (= 28992 (.getSrid ga)))))

(deftest invalid-srid-geometry-atrribute
  (let [geometry-map {"srid" "invalid srid" "wkt" wkt-multipolygon "type" "wkt"}]
    (is (thrown? NumberFormatException (create-geometry-attribute geometry-map)))))
