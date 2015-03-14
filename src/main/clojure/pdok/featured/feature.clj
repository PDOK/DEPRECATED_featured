(ns pdok.featured.feature
  (:refer-clojure :exclude [type]))

(defrecord NewFeature [dataset collection id validity geometry free-fields])
(defrecord ChangeFeature [dataset collection id validity current-validity geometry free-fields])
(defrecord CloseFeature [dataset collection id validity current-validity geometry free-fields])
(defrecord DeleteFeature [dataset collection id current-validity])

(defprotocol Geometry
  (src [_])
  (type [_])
  (as-gml [_]))

(defrecord GmlGeometry [src]
  Geometry
  (src [_] src)
  (type [_] :gml)
  (as-gml [_] src)
    )
