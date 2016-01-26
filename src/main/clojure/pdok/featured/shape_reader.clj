(ns pdok.featured.shape-reader
  (:require [clojure.java.io :as io])
  (:import [org.geotools.data DataStoreFinder DataStore]
           [org.opengis.feature.simple SimpleFeature]
           [org.opengis.feature.type AttributeDescriptor]))

(defn shape-file-store ^DataStore [path]
  (let [url (-> path io/file .toURI str)
        config {"url" url}]
    (DataStoreFinder/getDataStore config)))

(defn attributes [^SimpleFeature gt-feature]
  (let [attribute-names (filter #(not (clojure.string/includes? % "geom"))
                                (map (fn [^AttributeDescriptor d] (.getLocalName d))
                                     (-> gt-feature .getFeatureType .getAttributeDescriptors)))
        attributes (map (fn [^java.lang.String an] [an (.getAttribute gt-feature an)]) attribute-names)]
    attributes))

(defn make-feature [dataset layer ^SimpleFeature gt-feature]
  {:dataset dataset
   :collection layer
   :id (.getID gt-feature)
   :attributes (attributes gt-feature)
   :geometry {"type" "jts"  "jts" (.getDefaultGeometry gt-feature)}})

(defn feature-sources [^DataStore shape-store]
  "Returns seq of [layer-name featurecollection] "
  (let [layers (seq (.getTypeNames shape-store))
        sources (map (fn [^java.lang.String l] [l (-> shape-store
                                                     (.getFeatureSource l) .getFeatures)]) layers)]
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
