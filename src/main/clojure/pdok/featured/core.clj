(ns pdok.featured.core
  (:require [pdok.featured.json-reader :refer [features-from-stream file-stream]]
            [pdok.featured.processor :refer :all])
  (:gen-class))

(defn -main
  "first parameter is json file"
  [& args]
  (let [processor (processor) ]
    (with-open [s (file-stream (first args))]
      (time(doseq [feature (features-from-stream s)]
             (process processor feature))))
    (println "flushing.")
    (shutdown processor))
  (println "done.")
)


;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
