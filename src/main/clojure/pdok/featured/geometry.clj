(ns pdok.featured.geometry
  (:refer-clojure :exclude [type])
  (:require [clojure.string :as str])
  (:import []))

(defprotocol Geometry
  (src [_])
  (type [_])
  (as-gml [_]))

(defrecord GmlGeometry [src]
  Geometry
  (src [_] src)
  (type [_] :gml)
  (as-gml [_] src)
    )

(defmulti geometry-from-json (fn [obj] str/lower-case (get obj "type")))

(defmethod geometry-from-json :default [obj] (GmlGeometry. (get obj "gml")))
