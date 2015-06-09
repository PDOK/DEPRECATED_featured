(ns pdok.featured.mustache-functions
   (:require [pdok.featured.feature :as feature]))

(defn gml [arg] (feature/as-gml arg))