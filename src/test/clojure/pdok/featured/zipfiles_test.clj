(ns pdok.featured.zipfiles-test
  (:require [pdok.featured.zipfiles :as z]
            [clojure.java.io :as io]
            [clojure.test :refer :all]))

(deftest empty-zip
  (with-open [zip (io/input-stream (io/resource "zipfiles/empty.zip"))]
   (is (thrown-with-msg? Exception 
                         #"No entries in zip" 
                         (z/zip-as-input zip)))))

(deftest one-entry-zip 
  (with-open [zip (io/input-stream (io/resource "zipfiles/one-file.zip"))
              in (z/zip-as-input zip)]
    (is (boolean (re-find #"gml" (slurp in))))))