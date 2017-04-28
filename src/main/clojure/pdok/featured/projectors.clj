(ns pdok.featured.projectors
  (:refer-clojure :exclude [flush]))

(defprotocol Projector
  (init [persistence tx for-dataset current-collections])
  (new-collection [persistence collection])
  (flush [persistence])
  (new-feature [persistence feature])
  (change-feature [persistence feature])
  (close-feature [persistence feature])
  (delete-feature [persistence feature])
  (accept? [persistence feature])
  (close [persistence]))
