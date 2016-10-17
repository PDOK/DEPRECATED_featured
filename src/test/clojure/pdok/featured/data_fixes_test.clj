(ns pdok.featured.data-fixes-test
  (:require [clojure.test :refer :all]
            [pdok.featured.data-fixes :refer :all]))

;; Following two tests should have the old code to (transit) deserialize old database localmoment values
;; to correct original values. Use transit/from-json (maybe with dynamic or extra param,
;; for different lm deserializer
;; Use updated featured-shared before fixing this.
(deftest wrong-start-of-epoch-lm-deserialize
  "Check that [\"~#lm\",-3600000] deserializes to 1970-01-01T00:00:00.000")

(deftest wrong-summertime-lm-deserialize
  "Check that [\"~#lm\",1467324000000] deserializes to 2016-07-01T00:00:00.000" )