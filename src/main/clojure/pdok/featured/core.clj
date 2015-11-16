(ns pdok.featured.core
  (:require [pdok.featured
             [api :as api]
             [config :as config]
             [json-reader :refer [features-from-stream file-stream]]
             [processor :as processor :refer [consume shutdown]]
             [persistence :as pers]
             [generator :refer [random-json-feature-stream]]]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.tools.logging :as log])
  (:gen-class))

(declare cli-options)

(defn execute [{:keys [json-file
                       dataset-name
                       no-projectors
                       no-timeline
                       no-state
                       projection]}]
  (log/info (str "start" (when dataset-name (str " - dataset: " dataset-name)) " - file: " json-file
                 (when no-projectors " without projectors")
                 (when projection (str " as " projection))))
  (let [persistence (if no-state (pers/make-no-state) (config/persistence))
        projectors (cond-> [] (not no-projectors) (conj (config/projectors persistence projection))
                           (not (or no-timeline no-state)) (conj (config/timeline persistence)))
        processor (processor/create persistence projectors)]
    (with-open [s (if json-file (file-stream json-file) (clojure.java.io/reader *in*))]
      (dorun (consume processor (features-from-stream s :dataset dataset-name)))
      (do (log/info "Shutting down.")
          (shutdown processor))))
  (log/info "done")
  )

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn -main [& args]
;  (println "ENV-test" (config/env :pdok-test))
  (let [{:keys [options arguments summary options]} (parse-opts args cli-options)]
    (cond
      (:help options) (exit 0 summary)
      (not (or (:json-file options) (:std-in options))) (exit 0 "json-file or std-in required")
      :else (execute options))))

(def cli-options
  [["-f" "--json-file FILE" "required JSON-file with features"]
   [nil "--std-in" "Read from std-in"]
   ["-d" "--dataset-name DATASET" "dataset"]
   [nil "--no-projectors"]
   [nil "--no-timeline"]
   [nil "--no-state" "Also sets --no-timeline. Use only with no nesting and action :new only"]
   [nil "--projection PROJ" "RD / ETRS89 / SOURCE"]
   ["-h" "--help"]])

(defn performance-test [n & args]
  (with-open [json (apply random-json-feature-stream "perftest" "col1" n args)]
    (let [persistence (config/persistence)
          processor (processor/create persistence (config/projectors persistence))
          features (features-from-stream json)
          consumed (consume processor features)
          _ (println (map :invalid-reasons consumed))
          ]
      (time (do (log/info "Events processed:" (count consumed))
                (shutdown processor)
                ))
      )))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
