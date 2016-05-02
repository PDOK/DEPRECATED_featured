(ns pdok.featured.projectors
  (:refer-clojure :exclude [flush]))

(defprotocol Projector
  (init [persistence for-dataset collections])
  (new-collection [persistence collection parent-collection])
  (flush [persistence])
  (new-feature [persistence feature])
  (change-feature [persistence feature])
  (close-feature [persistence feature])
  (delete-feature [persistence feature])
  (accept? [persistence feature])
  (close [persistence]))
