(ns pdok.featured.mustache-functions
   (:require [pdok.featured.feature :as feature]
             [pdok.random :as random]
             [pdok.util :as util]))

(defn gml [arg] (feature/as-gml (util/as-geometry-attribute arg)))

(defn simple-gml [arg] (feature/as-simple-gml arg))

(defn stufgeo-field [arg] (feature/as-stufgeo-field arg))
(defn stufgeo-gml [arg] (feature/as-stufgeo-gml arg))
(defn stufgeo-gml-lc [arg] (feature/as-stufgeo-gml-lc arg))

(defn _version [arg]
  (if-let [version (:_version arg)]
    version
    (random/ordered-UUID)))
