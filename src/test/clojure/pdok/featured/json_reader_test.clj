(ns pdok.featured.json-reader-test
  (:require [pdok.featured.json-reader :refer :all]
            [clj-time.core :as time]
            [clojure.test :refer :all]))

(deftest date-time-with-timezone-to-local
  (let [datetimestring "2016-01-01T12:00:00.000+02:00"
        required (time/local-date-time 2016 1 1 12 0 0 0)
        parsed (parse-time datetimestring)]
    (is (= required parsed))))

(deftest date-time-without-timezone-to-local
  (let [datetimestring "2016-01-01T12:00:00.000"
        required (time/local-date-time 2016 1 1 12 0 0 0)
        parsed (parse-time datetimestring)]
    (is (= required parsed))))