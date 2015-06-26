(ns pdok.featured.config
  (:require [pdok.featured
             [persistence :as pers]
             [projectors :as proj]
             [timeline :as timeline]]
            [environ.core :refer [env]]))

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

(defn projectors [persistence]
  [(proj/geoserver-projector {:db-config data-db})
     (timeline/create {:db-config processor-db :persistence persistence})])
