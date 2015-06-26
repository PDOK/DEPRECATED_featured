(ns pdok.featured.core
  (:require [pdok.featured
             [api :as api]
             [config :as config]
             [json-reader :refer [features-from-stream file-stream]]
             [processor :as processor :refer [consume shutdown]]
             [generator :refer [random-json-feature-stream]]]
            [clojure.tools.cli :refer [parse-opts]]
            [compojure.core :refer [routes]]
            [ring.middleware.json :as middleware]
            [environ.core :refer [env]])
  (:gen-class))

(declare cli-options)


(defn execute [{:keys [json-file
                       dataset-name
                       no-projectors]}]
  (println (str "start" (when dataset-name " dataset: " dataset-name) (when no-projectors " without projectors")))
  (let [persistence (config/persistence)
        processor (if no-projectors
                    (processor/create persistence)
                    (processor/create persistence (config/projectors persistence)))]
    (with-open [s (file-stream json-file)]
      (let [consumed (consume processor (features-from-stream s :dataset dataset-name))]
        (time (do (println "EVENTS PROCESSED: " (count consumed))
                  (println "flushing.")
                  (shutdown processor))))))
  (println "done.")
)

(def app (->(routes (api/rest-handler nil))
            (middleware/wrap-json-body {:keywords? true :bigdecimals? true})
            (middleware/wrap-json-response)))

(defn -main [& args]
  (let [parameters (parse-opts args cli-options)]
    (execute (:options parameters))))

(def cli-options
  [["-f" "--json-file FILE" "JSON-file with features"]
   ["-d" "--dataset-name DATASET" "dataset"]
   ["-n" "--no-projectors"]])

(defn performance-test [n & args]
  (with-open [json (apply random-json-feature-stream "perftest" "col1" n args)]
    (let [persistence (config/persistence)
          processor (processor/create persistence (config/projectors persistence))
          features (features-from-stream json)
          consumed (consume processor features)
          ]
      (time (do (println "EVENTS PROCESSED: " (count consumed))
                (shutdown processor)
                ))
      )))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
