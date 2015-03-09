(ns pdok.featured.geometry
  (:require [clojure.string :as str])
  (:import []))

(defmulti geometry (fn [obj] str/lower-case (get obj "type")))

(defmethod geometry "gml" [obj] (get obj "gml"))
