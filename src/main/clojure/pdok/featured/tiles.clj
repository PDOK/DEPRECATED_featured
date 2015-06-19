(ns pdok.featured.tiles
  (:require [pdok.featured.feature :as f])
  (:import [pdok.featured.tiles NLTile]))

(defn nl [geometry]
  "Returns a set with NL tiles (numbers) based on the geometry."
  (when geometry
    (let [coordinates (.getCoordinates (f/as-jts geometry))]
      (into #{} (filter #(not= -1 %) (map (fn [coordinate] (NLTile/getTileFromRD (.x coordinate) (.y coordinate))) coordinates))))))
