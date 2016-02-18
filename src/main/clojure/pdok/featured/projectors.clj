(ns pdok.featured.projectors
  (:refer-clojure :exclude [flush]))

(defprotocol Projector
  (init [proj for-dataset])
  (flush [proj])
  (new-feature [proj feature])
  (change-feature [proj feature])
  (close-feature [proj feature])
  (delete-feature [proj feature])
  (accept? [proj feature])
  (close [proj]))
