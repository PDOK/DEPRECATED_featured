(ns pdok.featured.projectors)

(defprotocol Projector
  (init [proj])
  (new-feature [proj feature])
  (change-feature [proj feature])
  (close-feature [proj feature])
  (delete-feature [proj feature])
  (accept? [proj feature])
  (close [proj]))