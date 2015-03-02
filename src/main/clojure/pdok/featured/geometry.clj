(ns pdok.featured.geometry
  (:require [clojure.string :as str])
  (:import []))

(defmulti geometry (fn [obj] str/lower-case (:type obj)))

(defmethod geometry "gml" [obj] (:gml obj))
