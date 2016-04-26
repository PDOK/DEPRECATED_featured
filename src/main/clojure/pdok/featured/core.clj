(ns pdok.featured.core
  (:require [pdok.featured
             [api :as api]
             [config :as config]
             [json-reader :refer [features-from-stream file-stream]]
             [processor :as processor :refer [consume shutdown]]
             [persistence :as pers]
             [timeline :as tl]
             [generator :refer [random-json-feature-stream]]
             [extracts :as extracts]
             [template :as template]]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.tools.logging :as log])
  (:gen-class))

(declare cli-options features-from-files)

(defn setup-processor [{:keys [dataset
                               no-projectors
                               no-timeline
                               no-state
                               projection] :as meta}]
  (log/info (str "Configuring:" (when dataset (str " - dataset: " dataset))
                 (when no-projectors " without projectors")
                 (when no-state " without state")
                 (when projection (str " as " projection))))
  (let [persistence (if no-state (pers/make-no-state) (config/persistence))
        projectors (cond-> [] (not no-projectors) (conj (config/projectors persistence :projection projection))
                           (not no-timeline) (conj (config/timeline persistence)))
        processor (processor/create meta dataset persistence projectors)]
    processor))

(defn process [{:keys [json-files dataset] :as options}]
  (log/info "Processing mode")
  (let [[meta features] (features-from-files json-files :dataset dataset)
        processor (setup-processor (merge meta options))]
    (dorun (consume processor features))
    (do (log/info "Shutting down.")
        (shutdown processor)))
  (log/info "Done processing")
  )

(defn int? [^java.lang.String s]
  (try
    (Integer/parseInt s)
    true
    (catch Exception e false)))

(defn replay [{:keys [replay dataset] :as options}]
  (log/info "Replay mode")
  (let [n (when (int? replay) (Integer/parseInt replay))
        root-col (when-not (int? replay) replay)
        processor (setup-processor options)]
    (processor/replay processor n root-col)
    (log/info "Shutting down")
    (shutdown processor)
    (log/info "Done replaying")))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn implementation-version []
  (-> ^java.lang.Class (eval 'pdok.featured.core) .getPackage .getImplementationVersion))

(defn -main [& args]
;  (println "ENV-test" (config/env :pdok-test))
  (let [{:keys [options arguments summary]} (parse-opts args cli-options)]
    (cond
      (:help options)
        (exit 0 summary)
      (:version options)
        (exit 0 (implementation-version))
      (:replay options)
        (if (not (:dataset options))
          (exit 0 "replaying requires dataset")
          (replay options))
      (:clear-timeline-changelog options)
        (if (not (:dataset options))
          (exit 0 "clearing changelog requires dataset")
          (tl/delete-changelog (config/timeline-for-dataset (:dataset options))))
      (not (or (seq arguments) (:std-in options)))
        (exit 0 "json-file or std-in required")
      :else
        (let [files (if (:std-in options)
                      [*in*]
                      arguments)]
          (process (assoc options :json-files files))))))

(def cli-options
  [[nil "--std-in" "Read from std-in"]
   ["-d" "--dataset DATASET" "dataset"]
   [nil "--no-projectors"]
   [nil "--no-timeline"]
   [nil "--no-state" "Use only with no nesting and action :new"]
   [nil "--disable-validation"]
   [nil "--projection PROJ" "RD / ETRS89 / SOURCE"]
   ["-r" "--replay [N/root-collection]" "Replay last N events or all events from root-collection tree from persistence to projectors"]
   [nil "--clear-timeline-changelog" "Clear timeline changelog for dataset"]
   ["-h" "--help"]
   ["-v" "--version"]])

(defn performance-test [n & args]
  (with-open [^java.io.InputStream json (apply random-json-feature-stream "perftest" "col1" n args)]
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

(deftype CloseableSeq [delegate-seq close-fn]
  clojure.lang.ISeq
    (next [this]
      (if-let [n (next delegate-seq)]
        (CloseableSeq. n close-fn)
        (.close this)))
    (first [this] (if-let [f (first delegate-seq)] f (.close this)))
    (more [this] (if-let [n (next this)] n '()))
    (cons [this obj] (CloseableSeq. (cons obj delegate-seq) close-fn))
    (count [this] (count delegate-seq))
    (empty [this] (CloseableSeq. '() close-fn))
    (equiv [this obj] (= delegate-seq obj))
  clojure.lang.Seqable
    (seq [this] this)
  java.io.Closeable
    (close [this] (close-fn)))

(defn features-from-files [files & args]
  (when (seq files)
    (let [f (first files)
          fs (clojure.java.io/reader f)]
      (log/info "Processing " f)
      (concat (CloseableSeq. (apply features-from-stream f args) #(.close fs))
              (lazy-seq (apply features-from-files (rest files) args))))))
