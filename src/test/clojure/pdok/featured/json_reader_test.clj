(ns pdok.featured.json-reader-test
  (:require [pdok.featured.json-reader :refer :all]
            [clj-time.core :as time]
            [clojure.java.io :as io]
            [clojure.test :refer :all])
  (:import (pdok.featured GeometryAttribute)
           (org.joda.time IllegalInstantException)))

(deftest date-time-with-timezone-to-local
  (let [datetimestring "2016-01-01T12:00:00.000+01:00"
        required (time/local-date-time 2016 1 1 12 0 0 0)
        parsed (parse-time datetimestring)]
    (is (= required parsed))))

(deftest date-time-with-z-to-local
  (let [datetimestring "2016-01-01T12:00:00.000Z"
        required (time/local-date-time 2016 1 1 13 0 0 0)
        parsed (parse-time datetimestring)]
    (is (= required parsed))))

(deftest date-time-without-timezone-to-local
  (let [datetimestring "2016-01-01T12:00:00.000"
        required (time/local-date-time 2016 1 1 12 0 0 0)
        parsed (parse-time datetimestring)]
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

(deftest illegal-instant
  (is (thrown? IllegalInstantException (parse-time "1937-07-01T00:00:00.010")))
  (is (= (time/local-date-time 1937 7 1 0 0 0 10) (parse-time "1937-07-01T00:00:00.010" :no-timezone true)))
  (is (thrown? 
        IllegalInstantException 
        (-> "illegal-instant.json" 
          io/resource 
          io/input-stream
          features-from-stream
          second
          doall)))
  (is
    (let [[_ [feature & _]] (-> "illegal-instant.json"
                              io/resource 
                              io/input-stream
                              (features-from-stream :no-timezone true))
          date-time (time/local-date-time 1937 7 1 0 0 0 10)]
      (and
        (= 
          date-time
          (:validity feature))
        (=
          date-time
          (get feature "begindatumTijdvakGeldigheid")))))
  (is
    (let [[_ [feature & _]] (-> "valid-instant.json" 
                              io/resource 
                              io/input-stream
                              features-from-stream)
          date-time (time/local-date-time 1938 7 1 0 0 0 10)]
      (and
        (= 
          date-time
          (:validity feature))
        (=
          date-time
          (get feature "begindatumTijdvakGeldigheid"))))))
