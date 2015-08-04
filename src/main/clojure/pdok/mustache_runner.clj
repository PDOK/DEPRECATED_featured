(ns pdok.mustache-runner
  (:require [pdok.featured.extracts  :as e])
  (:gen-class))

(defn- write-file [dataset feature-type feature idx]
  (with-open [w (clojure.java.io/writer (str dataset "-" feature-type "-" idx ".xml"))]
       (.write w feature)))

(defn -main [feature-type feature-file extract-type & args]
  (let [dataset "bgt"
        features (e/file-to-features feature-file dataset)
        template-dir (or (first args) "")
        rendered-features (map second (e/features-for-extract dataset feature-type extract-type features template-dir))
        num-features (count rendered-features)]
    (doseq [idx (take num-features (range num-features))]
     (write-file "bgt" feature-type (get (vec rendered-features) idx) idx))))
