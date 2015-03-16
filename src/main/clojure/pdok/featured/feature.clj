(ns pdok.featured.feature
  (:refer-clojure :exclude [type])
  (:require [clojure.string :as str]))

(defrecord NewFeature [dataset collection id validity geometry attributes])
(defrecord ChangeFeature [dataset collection id validity current-validity geometry attributes])
(defrecord CloseFeature [dataset collection id validity current-validity geometry attributes])
(defrecord DeleteFeature [dataset collection id current-validity])

(defmulti as-gml (fn [obj] str/lower-case (get obj "type")))

(defmethod as-gml "gml" [obj] (get obj "gml"))
(defmethod as-gml nil [obj] nil)
