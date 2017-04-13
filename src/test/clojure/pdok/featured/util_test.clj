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

(deftest partitioned-test
  (let [n-calls (atom 0)
        ids (range 100)
        f (fn [id] [id (inc id)])
        mapper (fn [coll] (swap! n-calls inc) (map f coll))
        normal (mapper ids)
        parted (partitioned mapper 10 ids)]
    (is (= [0 1] (first normal)))
    (is (= (take 2 normal) '([0 1] [1 2])))
    (is (= normal parted))
    (is (= 11 @n-calls))))
