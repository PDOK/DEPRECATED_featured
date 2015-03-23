(ns pdok.featured.projectors
  (:require [pdok.featured.protocols :refer :all]
   [environ.core :refer [env]]))

(defprotocol Projector
  (new-feature [proj feature]))

(def ^:private pdok-db {:subprotocol "postgresql"
                     :subname (or (env :projector-database-url) "//localhost:5432/pdok")
                     :user (or (env :projector-database-user) "postgres")
                     :password (or (env :projector-database-password) "postgres")})

(deftype GeoserverProjector []
    Projector
    (new-feature [_ feature])
    Closeable
    (close [_]))
