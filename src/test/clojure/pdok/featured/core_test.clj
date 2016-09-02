(ns pdok.featured.core-test
  (:require [clojure.te¬st :refer :all]
            [pdok.featured.core :refer :all]))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 1 1))))

(defn test-main []
  (-main "-f" "D:\\data\\bgt\\mutatie-leveringen\\openbareruimtelabel\\973140-OpenbareRuimteLabel-1.json" "-d" "bgt"))


(defn entries [zipfile]
  (enumeration-seq (.entries zipfile)))

(defn walkzip [fileName]
  (with-open [z (java.util.zip.ZipFile. fileName)]
             (doseq [e (entries z)]
                    (println (.getName e)))))

(defn- file-name [file]
  (.getPath file))

(defn- ends-with-json [s]
  (.endsWith s ".json"))

(defn json-files [dir]
  (filter ends-with-json (map file-name (file-seq (java.io.File. dir)))))

(defn execute-main [file]
  (let [_ (println file)]
  (-main "-f" file "-d" "bgt")))
