(ns pdok.featured.core
  (:require [pdok.featured.feature :refer [features-from-package-stream file-stream]]
            [pdok.featured.store :refer [make-feature-store process shutdown]])
  (:gen-class))

(defn -main
  "first parameter is json file"
  [& args]
  (let [store (make-feature-store) ]
    (with-open [s (file-stream (first args))]
      (time(doseq [feature (features-from-package-stream s)]
             (process store feature))))
    (println "flushing.")
    (shutdown store))
  (println "done.")
)


;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
