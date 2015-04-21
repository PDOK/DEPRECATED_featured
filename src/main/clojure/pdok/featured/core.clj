(ns pdok.featured.core
  (:require [pdok.featured.json-reader :refer [features-from-stream file-stream]]
            [pdok.featured.processor :refer :all]
            [pdok.featured.projectors :as proj]
            [pdok.featured.generator :refer [random-json-feature-stream]]
            [clojure.tools.cli :refer [parse-opts]]
            [environ.core :refer [env]])
  (:gen-class))

(declare cli-options)

(def data-db {:subprotocol "postgresql"
                     :subname (or (env :data-database-url) "//localhost:5432/pdok")
                     :user (or (env :data-database-user) "postgres")
                     :password (or (env :data-database-password) "postgres")})

(defn execute [{:keys [json-file
                       dataset-name
                       no-projectors]}]
  (println (str "start" (when dataset-name " dataset: " dataset-name) (when no-projectors " without projectors")))
  (let [processor (if no-projectors
                    (processor [])
                    (processor [(proj/geoserver-projector {:db-config data-db})
                                (proj/parent-child-projector {:db-config data-db})]))]
    (with-open [s (file-stream json-file)]
      (let [consumed (consume processor (features-from-stream s :dataset dataset-name))]
        (time (do (println "EVENTS PROCESSED: " (count consumed))
                  (println "flushing.")
                  (shutdown processor))))))
  (println "done.")
)

(defn -main [& args]
  (let [parameters (parse-opts args cli-options)]
    (execute (:options parameters))))

(def cli-options
  [["-f" "--json-file FILE" "JSON-file with features"]
   ["-d" "--dataset-name DATASET" "dataset"]
   ["-n" "--no-projectors"]])

(defn performance-test [n & args]
  (with-open [json (apply random-json-feature-stream "perftest" "col1" n args)]
    (let [processor (processor [(proj/geoserver-projector {:db-config data-db})
                                (proj/parent-child-projector {:db-config data-db})])
          features (features-from-stream json)
          consumed (consume processor features)]
      (time (do (println "EVENTS PROCESSED: " (count consumed))
                (shutdown processor)
                ))
      )))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
