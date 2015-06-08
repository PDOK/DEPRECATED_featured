(ns pdok.featured.core
  (:require [pdok.featured.json-reader :refer [features-from-stream file-stream]]
            [pdok.featured.processor :as processor :refer [consume shutdown]]
            [pdok.featured.projectors :as proj]
            [pdok.featured.timeline :as timeline]
            [pdok.featured.persistence :as pers]
            [pdok.featured.generator :refer [random-json-feature-stream]]
            [clojure.tools.cli :refer [parse-opts]]
            [environ.core :refer [env]])
  (:gen-class))

(declare cli-options)

(def ^:private processor-db {:subprotocol "postgresql"
                     :subname (or (env :processor-database-url) "//localhost:5432/pdok")
                     :user (or (env :processor-database-user) "postgres")
                     :password (or (env :processor-database-password) "postgres")})

(def data-db {:subprotocol "postgresql"
                     :subname (or (env :data-database-url) "//localhost:5432/pdok")
                     :user (or (env :data-database-user) "postgres")
                     :password (or (env :data-database-password) "postgres")})

(defn execute [{:keys [json-file
                       dataset-name
                       no-projectors]}]
  (println (str "start" (when dataset-name " dataset: " dataset-name) (when no-projectors " without projectors")))
  (let [persistence (pers/cached-jdbc-processor-persistence
                    {:db-config processor-db :batch-size 10000})
        processor (if no-projectors
                    (processor/create persistence)
                    (processor/create persistence
                      [(proj/geoserver-projector {:db-config data-db})
                       (timeline/create {:db-config processor-db :persistence persistence})]))]
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
    (let [persistence (pers/cached-jdbc-processor-persistence {:db-config processor-db})
          processor (processor/create persistence
                      [(proj/geoserver-projector {:db-config data-db})
                       (timeline/create {:db-config processor-db :persistence persistence})])
          features (features-from-stream json)
          consumed (consume processor features)
         ; _ (doseq [c consumed] ( println c))
          ]
      (time (do (println "EVENTS PROCESSED: " (count consumed))
                (shutdown processor)
                ))
      )))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
