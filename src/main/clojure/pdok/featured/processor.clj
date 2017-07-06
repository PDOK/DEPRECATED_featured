(ns pdok.featured.processor
  (:refer-clojure :exclude [flatten])
  (:require [pdok.random :as random]
            [pdok.util :refer [with-bench]]
            [pdok.featured.persistence :as pers]
            [pdok.featured.json-reader :refer :all]
            [pdok.featured.projectors :as proj]
            [pdok.featured.config :as config]
            [clojure.core.async :as a]
            [clj-time [core :as t]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [pdok.featured.tiles :as tiles])
  (:import (pdok.featured.timeline Timeline ChunkedTimeline)))

(def ^:private pdok-fields [:action :id :dataset :collection :validity :version :current-validity :attributes :src])

(declare consume consume* process pre-process append-feature)

(def ^{:dynamic true} *next-version* random/ordered-UUID)   ;; generates uuid

(defn make-seq [obj]
  (if (seq? obj) obj (list obj)))

(defn- make-invalid [feature reason]
  (let [current-reasons (or (:invalid-reasons feature) [])
        new-reasons (conj current-reasons reason)]
    (-> feature (assoc :invalid? true) (assoc :invalid-reasons new-reasons))))

(defn- apply-all-features-validation [persistence {:keys [invalids]} feature]
  (let [{:keys [collection id action validity geometry attributes]} feature]
    (cond-> feature
      (or (some str/blank? [collection id]) (and (not= action :delete) (nil? validity)))
      (make-invalid "All feature require: collection id validity")
      (@invalids [(:collection feature) (:id feature)])
      (make-invalid "Feature marked invalid"))))

(defn- stream-exists? [persistence {:keys [collection id]}]
  (and (pers/stream-exists? persistence collection id)
       (not= :delete (pers/last-action persistence collection id))))

(defn- apply-new-feature-requires-non-existing-stream-validation [persistence feature]
  (let [{:keys [collection id]} feature]
    (if (and (not (:invalid? feature))
             (stream-exists? persistence feature))
      (make-invalid feature (str "Stream already exists: " collection ", " id))
      feature)))

(defn- apply-non-new-feature-requires-existing-stream-validation [persistence feature]
  (let [{:keys [collection id]} feature]
    (if (and  (not (:invalid? feature))
              (not (stream-exists? persistence feature)))
      (make-invalid feature (str "Stream does not exist yet: " collection ", " id))
      feature)))

(defn- apply-non-new-feature-current-validity<=validity-validation [feature]
  (let [{:keys [validity current-validity]} feature]
    (if (and validity (t/before? validity current-validity))
      (make-invalid feature "Validity should >= current-validity")
      feature)))

(defn- apply-non-new-feature-current-validity-validation [persistence feature]
  (let [{:keys [collection id validity current-validity]} feature]
    (if-not current-validity
      (make-invalid feature "Non new feature requires: current-validity")
      (let [stream-validity (pers/current-validity persistence collection id)]
        (if  (not= current-validity stream-validity)
          (make-invalid feature
                        (str  "When updating current-validity should match" current-validity " != " stream-validity))
          (apply-non-new-feature-current-validity<=validity-validation feature))))))

(defn- apply-closed-feature-cannot-be-changed-validation [persistence feature]
  (let [{:keys [collection id]} feature
        last-action (pers/last-action persistence collection id)]
    (if (= :close last-action)
      (make-invalid feature "Closed features cannot be altered")
      feature)))

(defn- validate [{:keys [persistence invalids] :as processor} feature]
  "Validates features."
  (let [validated
        (condp contains? (:action feature)
          #{:new}
          (->> feature
               (apply-all-features-validation persistence processor)
               (apply-new-feature-requires-non-existing-stream-validation persistence))
          #{:change}
          (->> feature
               (apply-all-features-validation persistence processor)
               (apply-non-new-feature-requires-existing-stream-validation persistence)
               (apply-closed-feature-cannot-be-changed-validation persistence)
               (apply-non-new-feature-current-validity-validation persistence))
          #{:new|change}
          (->> feature
               (apply-all-features-validation persistence processor))
          #{:close}
          (->> feature
               (apply-all-features-validation persistence processor)
               (apply-closed-feature-cannot-be-changed-validation persistence)
               (apply-non-new-feature-requires-existing-stream-validation persistence)
               (apply-non-new-feature-current-validity-validation persistence))
          #{:delete}
          (cond->> feature
            true (apply-all-features-validation persistence processor)
            (:check-existence-on-delete processor)
            (apply-non-new-feature-requires-existing-stream-validation persistence)
            (:check-validity-on-delete processor)
            (apply-non-new-feature-current-validity-validation persistence))
          feature)]
    (when (:invalid? validated)
      (vswap! invalids conj [(:collection validated) (:id validated)]))
    validated))

(defn- current-validity [persistence feature]
  (pers/current-validity persistence (:collection feature) (:id feature)))

(defn- with-current-version [persistence feature]
  (if-not (:invalid? feature)
    (let [{:keys [collection id]} feature]
      (assoc feature :current-version (pers/current-version persistence collection id)))
    feature))

(defn project! [processor proj-fn & more]
  (doseq [p (:projectors processor)]
    (apply proj-fn p more)))

(defn doto-projectors! [processor action-fn]
  (doseq [p (:projectors processor)]
    (action-fn p)))

(defn- process-new-feature [{:keys [persistence] :as processor} feature]
  (let [{:keys [collection id]} feature]
    (when-not (pers/collection-exists? persistence collection)
      (pers/create-collection persistence collection)
      (project! processor proj/new-collection collection))
    (when-not (pers/stream-exists? persistence collection id)
      (pers/create-stream persistence collection id))
    (append-feature persistence feature)
    (project! processor proj/new-feature feature))
  feature)

(defn- process-change-feature [{:keys [persistence] :as processor} feature]
  (let [enriched-feature (with-current-version persistence feature)
        current-validity (current-validity persistence feature)]
    (append-feature persistence enriched-feature)
    (if (t/before? current-validity (:validity feature))
      (let [close-feature (-> feature
                              (assoc :action :close)
                              (assoc :attributes {})
                              (dissoc :src))
            new-feature (-> feature
                            (assoc :action :new)
                            (assoc :version (*next-version*)))]
        (project! processor proj/close-feature close-feature)
        (project! processor proj/new-feature new-feature))
      (project! processor proj/change-feature enriched-feature))
    enriched-feature))

(defn- process-new|change-feature [{:keys [persistence] :as processor} feature]
  (if (stream-exists? persistence feature)
    (process-change-feature processor (assoc feature :action :change))
    (process-new-feature processor (assoc feature :action :new))))

(defn- process-close-feature [{:keys [persistence] :as processor} feature]
  (if (empty? (:attributes feature))
    (let [enriched-feature (with-current-version persistence feature)]
      (append-feature persistence enriched-feature)
      (project! processor proj/close-feature enriched-feature)
      (list enriched-feature))
    (let [current-validity (current-validity persistence feature)
          change-feature (-> feature
                             (assoc :action :change)
                             (assoc :validity current-validity)
                             (dissoc :src))
          close-feature (-> feature
                            (assoc :attributes {})
                            (assoc :version (*next-version*)))]
      (list
        (process-change-feature processor change-feature)
        (process-close-feature processor close-feature)))))

(defn- process-delete-feature [{:keys [persistence] :as processor} feature]
  (let [enriched-feature (with-current-version persistence feature)]
    (append-feature persistence enriched-feature)
    (project! processor proj/delete-feature enriched-feature)
    enriched-feature))

(defn- attributes [obj]
  (apply dissoc obj pdok-fields))

(defn- collect-attributes [feature]
  "Returns a feature where the attributes are collected in :attributes"
  (let [collected (merge (:attributes feature) (attributes feature))
        no-attributes (select-keys feature pdok-fields)
        collected (assoc no-attributes :attributes collected)]
    collected))

(defn process [processor feature]
  "Processes feature events. Should return the feature, possibly with added data, returns sequence"
  (let [validated (if (:disable-validation processor) feature (validate processor feature))]
    (if (:invalid? validated)
      (make-seq validated)
      (let [vf (assoc validated
                      :version (*next-version*)
                      :tiles (apply clojure.set/union
                                    (->> feature :attributes vals
                                      (filter #(instance? pdok.featured.GeometryAttribute %))
                                      (map tiles/nl))))
            processed
            (condp = (:action vf)
              :new (process-new-feature processor vf)
              :change (process-change-feature processor vf)
              :new|change (process-new|change-feature processor vf)
              :close (process-close-feature processor vf)
              :delete (process-delete-feature processor vf)
              (make-invalid vf (str "Unknown action:" (:action vf))))]
        (make-seq processed)))))

(defn- append-feature [persistence feature]
  (let [{:keys [version action collection id validity attributes]} feature]
    (pers/append-to-stream persistence version action collection id validity attributes)
    feature))

(defn- update-statistics [{:keys [statistics]} feature]
  (when statistics
    (swap! statistics update :n-processed inc)
    (when (:src feature) (swap! statistics update :n-src inc))
    (swap! statistics update :updated-tiles #(clojure.set/union % (:tiles feature)))
    (when (:invalid? feature)
      (swap! statistics update :n-errored inc)
      (swap! statistics update :errored #(conj % (:id feature))))))

(defn consume* [processor features]
  (letfn [(consumer [feature]
            (let [consumed (filter (complement nil?) (process processor feature))]
              (doseq [f consumed]
                (if (:invalid? f) (log/warn (:id f) (:invalid-reasons f)))
                (update-statistics processor f))
              consumed))]
    (mapcat consumer features)))

(defn consume-partition [processor features]
  (when (seq? features)
    (let [prepped (with-bench t (log/debug "Preprocessed batch in" t "ms")
                    (map collect-attributes features))
          _ (pers/flush (:persistence processor)) ;; flush before run, to save cache in shutdown
          _ (pers/prepare (:persistence processor) prepped)
          consumed (with-bench t (log/debug "Consumed batch in" t "ms")
                     (doall (consume* processor prepped)))
          _ (doto-projectors! processor proj/flush)]
      consumed)))

(defn consume [processor features]
  (let [n (:batch-size processor)]
    (when (seq features)
      (concat (consume-partition processor (take n features))
              (lazy-seq (consume processor (drop n features)))))))

(defn replay [{:keys [persistence projectors batch-size statistics] :as processor}
              last-n root-collection]
  "Sends the last n events from persistence to the projectors"
  (let [collections [root-collection]
        features (pers/get-last-n persistence last-n collections)
        parts (a/pipe features (a/chan 1 (partition-all batch-size)))]
    (loop [part (a/<!! parts)]
      (when (seq part)
        (pers/flush persistence)
        (pers/prepare persistence part)
        (doseq [f part]
          (condp = (:action f)
            :new (project! processor proj/new-feature f)
            :change (project! processor proj/change-feature f)
            :close (project! processor proj/close-feature f)
            :delete (project! processor proj/delete-feature f))
          (swap! statistics update :replayed inc))
        (doto-projectors! processor proj/flush)
        (recur (a/<!! parts))))))

(defn shutdown [{:keys [persistence projectors statistics] :as processor}]
  "Shutdown feature store. Make sure all data is processed and persisted"
  (let [_ (doto-projectors! processor proj/close)
        ;; timeline uses persistence, close persistence after projectors
        closed-persistence (pers/close persistence)
        timeline (first (filter #(or (instance? Timeline %1) (instance? ChunkedTimeline %1)) projectors))
        info {:persistence closed-persistence
              :projectors  projectors
              :statistics  (cond-> (if statistics @statistics {})
                                   timeline (assoc :changelogs @(:changelogs timeline)))}
        _ (when statistics (log/info @statistics))]
    info))

(defn add-projector [processor projector tx]
  (let [initialized-projector (proj/init projector tx (:dataset processor) (pers/collections (:persistence processor)))]
    (update-in processor [:projectors] conj initialized-projector)))

(defn create
  ([dataset persistence] (create dataset persistence []))
  ([dataset persistence projectors] (create {} dataset persistence projectors))
  ([options dataset persistence projectors]
   {:pre [(map? options) (string? dataset)]}
   (let [initialized-persistence (pers/init persistence dataset)
         tx (:tx initialized-persistence)
         initialized-projectors (doall (map #(proj/init % tx dataset (pers/collections initialized-persistence))
                                            (clojure.core/flatten projectors)))
         batch-size (or
                      (when-let [processor-batch-size (config/env :processor-batch-size)]
                        (Integer/parseInt processor-batch-size))
                      10000)]
     (merge {:dataset dataset
             :check-validity-on-delete true
             :check-existence-on-delete true
             :disable-validation false
             :persistence initialized-persistence
             :projectors initialized-projectors
             :batch-size batch-size
             :invalids (volatile! #{})
             :statistics (atom {:n-src 0 :n-processed 0 :n-errored 0 :errored '() :replayed 0 :updated-tiles #{}})}
            options))))
