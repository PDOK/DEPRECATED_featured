(ns pdok.featured.core
  (:require [pdok.featured.json-reader :refer [features-from-stream file-stream]]
            [pdok.featured.processor :refer :all]
            [pdok.featured.projectors :as proj]
            [clojure.tools.cli :refer [parse-opts]]
            [environ.core :refer [env]])
  (:gen-class))

(declare cli-options)

(defn execute [{:keys [json-file
                       dataset-name
                       no-projectors]}]
  (println (str "start" (when dataset-name " dataset: " dataset-name) (when no-projectors " without projectors")))
  (let [processor (if (nil? no-projectors)
                    (processor [(proj/geoserver-projector {:db-config proj/data-db})])
                    (processor [] ))
        dataset dataset-name]
    (with-open [s (file-stream json-file)]
      (time (do (doseq [feature (features-from-stream s :dataset dataset)]
                  (process processor feature))
                (println "flushing.")
                (shutdown processor)))))
  (println "done.")
)

(defn -main [& args]
  (let [parameters (parse-opts args cli-options)]
    (execute (parameters :options))))

(def cli-options
  [["-f" "--json-file FILE" "JSON-file with features"]
   ["-d" "--dataset-name DATASET" "dataset"]
   ["-n" "--no-projectors"]])

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
