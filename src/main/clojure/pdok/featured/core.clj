(ns pdok.featured.core
  (:require [pdok.featured.json-reader :refer [features-from-stream file-stream]]
            [pdok.featured.processor :refer :all]
            [pdok.featured.projectors :refer [projectors]])
  (:gen-class))

(defn no-projectors? [switch]
  (= switch "no-projectors"))

(defn execute [jsonfile projectors-switch]
  (println "start")
  (let [processor (if (no-projectors? projectors-switch)
                    (processor [] )
                    (processor projectors)) ]
    (with-open [s (file-stream jsonfile)]
      (time (do (doseq [feature (features-from-stream s)]
                  (process processor feature))
                (println "flushing.")
                (shutdown processor)))))
  (println "done.")
)

(defn -main
  "first parameter is json file, optional second parameter switch projectors"
  [& args]
  (execute (first args) (second args)))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
