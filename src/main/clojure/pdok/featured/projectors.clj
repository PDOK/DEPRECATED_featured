(ns pdok.featured.projectors
  (:require [environ.core :refer [env]]))

(defprotocol Projector
  (new-feature [proj feature])
  (close [proj]))

(deftype GeoserverProjector []
    Projector
    (new-feature [_ feature])
    (close [_]))
