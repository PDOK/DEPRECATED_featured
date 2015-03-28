(ns pdok.featured.processor
  (:require [pdok.featured.feature :as feature]
            [pdok.featured.persistence :as pers]
            [pdok.featured.generator :refer [random-json-feature-stream]]
            [pdok.featured.json-reader :refer :all]
            [pdok.featured.projectors :as proj]
            [clj-time [local :as tl]]
            [environ.core :refer [env]])
  (:import  [pdok.featured.projectors GeoserverProjector]))

(def ^:private processor-db {:subprotocol "postgresql"
                     :subname (or (env :processor-database-url) "//localhost:5432/pdok")
                     :user (or (env :processor-database-user) "postgres")
                     :password (or (env :processor-database-password) "postgres")})

(defn- process-new-feature [{:keys [persistence projectors]} feature]
  (let [{:keys [dataset collection id validity geometry attributes]} feature]
    (if (some nil? [dataset collection id validity geometry])
      "New feature requires: dataset collection id validity geometry"
      (if (pers/stream-exists? persistence dataset collection id)
        (str "Stream already exists: " dataset ", " collection ", " id)
        (do (pers/create-stream persistence dataset collection id)
            (pers/append-to-stream persistence :new dataset collection id validity geometry attributes)
            (doseq [p projectors] (proj/new-feature p feature)))))))

(defn- process-change-feature [{:keys [persistence projectors]} feature]
  (let [{:keys [dataset collection id current-validity validity]} feature]
    (if (some nil? [dataset collection id current-validity validity])
      "Change feature requires dataset collection id current-validity validity"
      (let [geometry (:geometry feature)]
        (if (and (contains? feature :geometry) (nil? geometry))
          "Change feature cannot have nil geometry (also remove the key)"
          (if (not (pers/stream-exists? persistence dataset collection id))
            "Create feature stream first with :new"
            (let [stream-validity (pers/current-validity persistence dataset collection id)]
              (if (not= current-validity stream-validity)
                "When changing current-validity should match"
                (do (pers/append-to-stream persistence :change dataset collection
                                           id validity geometry (:attributes feature)))))))))))

(defn process [processor feature]
  "Processes feature event. Returns nil or error reason"
  (condp = (:action feature)
    :new (process-new-feature processor feature)
    :change (process-change-feature processor feature)
    (str "Cannot process: " feature)))

(defn shutdown [{:keys [persistence projectors]}]
  "Shutdown feature store. Make sure all data is processed and persisted"
  (if-not persistence "persistence needed")
  (pers/close persistence)
  (doseq [p projectors] (proj/close p)))

(defn processor
  ([projectors]
   (let [jdbc-persistence (pers/cached-jdbc-processor-persistence {:db-config processor-db :batch-size 10000})]
    (processor jdbc-persistence projectors)))
  ([persistence projectors]
   {:persistence persistence
    :projectors projectors}))

(defn performance-test [count with-update]
  (with-open [json (random-json-feature-stream "perftest" "col1" count with-update)]
    (let [processor (processor [(proj/geoserver-projector {:db-config proj/data-db})])
          features (features-from-stream json)]
      (time (do (doseq [f features] (process processor f))
                (shutdown processor)
                ))
      )))


; features-from-stream
