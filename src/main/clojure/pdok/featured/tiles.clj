(ns pdok.featured.tiles
  (:require [pdok.featured.feature :as f])
  (:import [pdok.featured.tiles NLTile]))

(defn nl [feature]
  "Returns a set with tiles (numbers) based on the geometry in feature."
  (if-let [geometry (:geometry feature)]
    (let [coordinates (.getCoordinates (f/as-jts geometry))]
      (into #{} (map (fn [coordinate] (NLTile/getTileFromRD (.x coordinate) (.y coordinate))) coordinates)))))
