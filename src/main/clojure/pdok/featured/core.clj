(ns pdok.featured.core
  (:require [pdok.featured
             [api :as api]
             [config :as config]
             [json-reader :refer [features-from-stream file-stream]]
             [processor :as processor :refer [consume shutdown]]
             [persistence :as pers]
             [generator :refer [random-json-feature-stream]]
             [extracts :as extracts]
             [template :as template]]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.tools.logging :as log])
  (:gen-class))

(declare cli-options)

(defn execute [{:keys [json-files
                       dataset-name
                       no-projectors
                       no-timeline
                       no-state
                       projection]}]
  (log/info (str "start" (when dataset-name (str " - dataset: " dataset-name))
                 (when no-projectors " without projectors")
                 (when no-state " without state")
                 (when projection (str " as " projection))))
  (let [persistence (if no-state (pers/make-no-state) (config/persistence))
        projectors (cond-> [] (not no-projectors) (conj (config/projectors persistence projection))
                           (not no-timeline) (conj (config/timeline persistence)))
        processor (processor/create persistence projectors)]
    (doseq [json-file json-files]
      (log/info "Processing " json-file)
      (with-open [s (clojure.java.io/reader json-file)]
        (dorun (consume processor (features-from-stream s :dataset dataset-name)))))
    (do (log/info "Shutting down.")
        (shutdown processor)))
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
      (not (or (seq arguments) (:std-in options))) (exit 0 "json-file or std-in required")
      :else
      (let [files (if (:std-in options)
                    [*in*]
                    arguments)]
        (execute (assoc options :json-files files))))))

(def cli-options
  [[nil "--std-in" "Read from std-in"]
   ["-d" "--dataset-name DATASET" "dataset"]
   [nil "--no-projectors"]
   [nil "--no-timeline"]
   [nil "--no-state" "Use only with no nesting and action :new"]
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

(defn add-templates-and-create-extract [template-location dataset collection extract-type extract-version]
  (let [templates-with-metadata (template/templates-with-metadata dataset template-location)]
    (do 
      (doseq [t templates-with-metadata] 
        (template/add-or-update-template t))
      (extracts/fill-extract dataset collection extract-type (read-string extract-version)))))
 
;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
