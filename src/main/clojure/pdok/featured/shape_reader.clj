(ns pdok.featured.shape-reader
  (:require [clojure.java.io :as io])
  (:import [org.geotools.data DataStoreFinder]))

(defn shape-file-store [path]
  (let [url (-> path io/file .toURI str)
        config {"url" url}]
    (DataStoreFinder/getDataStore config)))

(defn attributes [gt-feature]
  (let [attribute-names (filter #(not (.contains % "geom"))
                                (map #(.getLocalName %) (-> gt-feature .getFeatureType .getAttributeDescriptors)))
        attributes (map #(vector %1 (.getAttribute gt-feature %1)) attribute-names)]
    attributes))

(defn make-feature [dataset layer gt-feature]
  {:dataset dataset
   :collection layer
   :id (.getID gt-feature)
   :attributes (attributes gt-feature)
   :geometry {"type" "jts"  "jts" (.getDefaultGeometry gt-feature)}})

(defn feature-sources [shape-store]
  "Returns seq of [layer-name featurecollection] "
  (let [layers (seq (.getTypeNames shape-store))
        sources (map #(vector %1 (-> shape-store (.getFeatureSource %1) .getFeatures)) layers)]
    sources))

(defmacro with-shape-features
  "Evaluates the body given features extracted from the shapefile:
  (with-shape-features [path dataset features] ... features ...).
  Might evaluate multiple times"
  [binding & body]
  `(let [path# ~(first binding)
         dataset# ~(second binding)
         store# (shape-file-store path#)
         sources# (feature-sources store#)]
     (try
       (doseq [[layer# gt-features#] sources#]
         (with-open [iterator# (.features gt-features#)]
           (while (.hasNext iterator#)
             (let [gt-feature# (.next iterator#)
                   ~(nth binding 2) (list (make-feature dataset# layer# gt-feature#))]
               ~@body))))
       (finally (.dispose store#))))
  )
