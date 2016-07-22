(ns pdok.featured.util-test
  (:require [clojure.test :refer :all]
            [pdok.util :refer :all]))

(defn return [val]
  val)

(-> checked-ok
    (deftest
      (is (= nil (checked (/ 1 0) (return true))))))

(deftest checked-real-error
  (is (thrown? ArithmeticException (checked (/ 1 0) (return false)))))