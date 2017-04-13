(ns pdok.featured.core-test
  (:require [clojure.test :refer :all]
            [pdok.featured.core :refer :all])
  (:import (java.io File)
           (java.util.zip ZipFile)))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 1 1))))

(defn test-main []
  (-main "-f" "D:\\data\\bgt\\mutatie-leveringen\\openbareruimtelabel\\973140-OpenbareRuimteLabel-1.json" "-d" "bgt"))

(defn entries [^ZipFile zipfile]
  (enumeration-seq (.entries zipfile)))

(defn walkzip [^String fileName]
  (with-open [z (ZipFile. fileName)]
    (doseq [e (entries z)]
      (println (.getName e)))))

(defn- file-name [^File file]
  (.getPath file))

(defn- ends-with-json [^String s]
  (.endsWith s ".json"))

(defn json-files [^String dir]
  (filter ends-with-json (map file-name (file-seq (File. dir)))))

(defn execute-main [file]
  (let [_ (println file)]
    (-main "-f" file "-d" "bgt")))
