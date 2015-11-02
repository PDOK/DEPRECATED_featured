(ns pdok.featured.config
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [pdok.featured
             [persistence :as pers]
             [projectors :as proj]
             [timeline :as timeline]]
            [environ.core :as environ]))

(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread throwable]
     (log/error throwable "Stacktrace:"
                (print-str (clojure.stacktrace/print-stack-trace throwable))))))

(defn- keywordize [s]
  (-> (str/lower-case s)
      (str/replace "_" "-")
      (str/replace "." "-")
      (keyword)))

(defn load-props [resource-file]
  (with-open [^java.io.Reader reader (io/reader (io/resource resource-file))]
    (let [props (java.util.Properties.)
          _ (.load props reader)]
      (into {} (for [[k v] props
                     ;; no mustaches, for local use
                     :when (not (re-find #"^\{\{.*\}\}$" v))]
                 [(keywordize k) v])))))

(defonce env
  (merge environ/env
         (load-props "plp.properties")))

(def processor-db {:subprotocol "postgresql"
                     :subname (or (env :processor-database-url) "//localhost:5432/pdok")
                     :user (or (env :processor-database-user) "postgres")
                     :password (or (env :processor-database-password) "postgres")})

(def data-db {:subprotocol "postgresql"
                     :subname (or (env :data-database-url) "//localhost:5432/pdok")
                     :user (or (env :data-database-user) "postgres")
                     :password (or (env :data-database-password) "postgres")})

(defn persistence []
  (pers/cached-jdbc-processor-persistence
                    {:db-config processor-db :batch-size 10000}))

(defn timeline
  ([]
   (timeline (persistence)))
  ([persistence]
    (timeline/create-chunked {:db-config processor-db :persistence persistence})))

(defn projectors [persistence]
  [(proj/geoserver-projector {:db-config data-db})])
