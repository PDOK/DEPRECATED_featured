(ns pdok.featured.processor
  (:require [pdok.featured.feature :refer [->NewFeature]]
            [pdok.featured.persistence :refer :all]
            [clj-time [local :as tl]]
            [environ.core :refer [env]])
  (:import  [pdok.featured.feature NewFeature ChangeFeature CloseFeature DeleteFeature]))

(def ^:private pgdb {:subprotocol "postgresql"
                     :subname (or (env :database-url) "//localhost:5432/pdok")
                     :user (or (env :database-user) "postgres")
                     :password (or (env :database-password) "postgres")})

(defn- process-new-feature [persistence {:keys [dataset collection id validity geometry attributes]}]
  (if (some nil? [dataset collection id validity])
    "NewFeature requires: dataset collection id validity")
  (let [exists? (:stream-exists? persistence)
        create  (:create-stream persistence)
        append  (:append-to-stream persistence)]
    (if (exists? dataset collection id)
      (str "Stream already exists: " dataset ", " collection ", " id)
      (do (create dataset collection id)
          (append :new dataset collection id validity geometry attributes)))))

(defn process [{:keys [persistence]} feature]
  "Processes feature event. Returns nil or error reason"
  (condp instance? feature
    NewFeature (process-new-feature persistence feature)
    (str "Cannot process: " feature)))

(defn shutdown [{:keys [persistence]}]
  "Shutdown feature store. Make sure all data is processed and persisted"
  (if-not persistence "persistence needed")
  ((:shutdown persistence)))

(defn processor
  ([] (let [jdbc-persistence (processor-cached-jdbc-persistence {:db-config pgdb :batch-size 10000})]
        (processor jdbc-persistence)))
  ([persistence]
   {:persistence persistence}))

(defn performance-test [count]
  (let [store (processor)
        features (map (fn [i] ( ->NewFeature "test" "test" (str i) (tl/local-now) nil {})) (range count))]
    (time (do (doseq [f features] (process store f))
              (shutdown store)))))
