(ns pdok.featured.mustache-functions
   (:require [pdok.featured.feature :as feature]
             [pdok.random :as random]))

(defn gml [arg] (feature/as-gml arg))

(defn simple-gml [arg] (feature/as-simple-gml arg))

(defn _version [arg]
  (if-let [version (:_version arg)]
    version
    (random/UUID)))
