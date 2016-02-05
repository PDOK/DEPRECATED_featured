(ns pdok.featured.zipfiles-test
  (:require [pdok.featured.zipfiles :as z]
            [clojure.java.io :as io]
            [clojure.test :refer :all]))

(defmacro with-resource-as-file [resource file-var & body]
  `(let [~file-var (java.io.File/createTempFile "featured-test" ".zip")]
    (with-open [zip# (io/input-stream (io/resource ~resource))]
      (io/copy zip# ~file-var))
    ~@body
    (io/delete-file ~file-var)
    ))

(deftest empty-zip
  (with-resource-as-file "zipfiles/empty.zip" file
    (is (= nil (z/first-file-from-zip file)))))

(deftest one-entry-zip
  (with-resource-as-file "zipfiles/one-file.zip" file
    (let [entry (z/first-file-from-zip file)]
      (is (boolean (re-find #"gml" (slurp entry)))))))
