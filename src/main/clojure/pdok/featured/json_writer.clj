(ns pdok.featured.json-writer
  (:require [pdok.featured.persistence :as pers]
            [pdok.featured.projectors :as proj]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clj-time [coerce :as tc]])
  (:import (pdok.featured NilAttribute)
           (java.io Writer)
           (java.util.zip ZipOutputStream ZipEntry)
           (org.joda.time LocalDate DateTime LocalDateTime)))

(defn- get-value [fix-timezone key value]
  (if (instance? NilAttribute value)
    (case (.clazz value)
      LocalDate ["~#date" nil]
      DateTime ["~#moment" nil]
      LocalDateTime ["~#moment" nil]
      nil)
    (if (or (= key "_validity") (= key "_current_validity")) ;; Validities are plain strings, not ~#moment
      (str value)
      (if (instance? LocalDate value)
        ["~#date" [(str value)]] ;; The exact timestamp for dates is also different if fix-timezone is true,
                                 ;; but they're deserialized (rounded) to the same date as when fix-timezone is false
        (if (instance? LocalDateTime value)
          ["~#moment" [(str (if fix-timezone
                              (LocalDateTime. (tc/to-long value))
                              value))]]
          value)))))

(defn- merge-if-not-nil [coll name field]
  (merge coll (if (not (nil? field))
                {name field}
                nil)))

(defn- fix-timezone? [feature]
  (let [validity (tc/to-long (:validity feature))
        lv_publicatiedatum (tc/to-long (get (:attributes feature) "lv-publicatiedatum"))
        offset (if (and (not (nil? validity)) (not (nil? lv_publicatiedatum)))
                 (- validity lv_publicatiedatum)
                 0)]
    (> offset 999)))

(defn- write-feature [^Writer writer feature]
  (.write writer
          ^String
            (json/write-str
              (-> {"_action" (:action feature)
                   "_collection" (:collection feature)
                   "_id" (:id feature)}
                  (merge-if-not-nil "_validity" (:validity feature)) ;; nil for delete
                  (merge-if-not-nil "_current_validity" (:current-validity feature)) ;; nil for new
                  (merge-if-not-nil "_geometry" (:geometry feature))
                  (merge-if-not-nil "_parent_collection" (:parent_collection feature))
                  (merge-if-not-nil "_parent_id" (:parent_id feature))
                  (merge-if-not-nil "_parent_field" (:parent_field feature))
                  (merge (:attributes feature)))
              :value-fn (partial get-value (fix-timezone? feature)))))

(def is-first (atom true))

(defn- start-file [zip writer]
  (.putNextEntry zip (ZipEntry. (str "part" (System/currentTimeMillis) ".json")))
  (.write writer "{\"features\":[")
  (swap! is-first (fn [_] false)))

(defn- end-file [zip writer]
  (.write writer "]}\n")
  (.flush writer)
  (.closeEntry zip)
  (swap! is-first (fn [_] true)))

(defrecord JsonWriterProjector [path-fn filename]
  proj/Projector
  (proj/init [this _ _]
    (let [file (io/output-stream filename)
          zip (ZipOutputStream. file)
          writer (io/writer zip)]
      (-> this (assoc :file file) (assoc :zip zip) (assoc :writer writer))))
  (proj/new-collection [_ _ _])
  (proj/flush [this]
    (when (not @is-first)
      (end-file (:zip this) (:writer this))))
  (proj/new-feature [this feature]
    (if @is-first
      (start-file (:zip this) (:writer this))
      (.write (:writer this) ",\n"))
    (let [[parent_collection parent_id parent_field _] (last (path-fn (:collection feature) (:id feature)))
          feature-with-parent (merge feature {:parent_collection parent_collection
                                              :parent_id parent_id
                                              :parent_field parent_field})]
      (write-feature (:writer this) feature-with-parent)))
  (proj/change-feature [this feature]
    (proj/new-feature this feature))
  (proj/close-feature [this feature]
    (proj/new-feature this feature))
  (proj/delete-feature [this feature]
    (proj/new-feature this feature))
  (proj/accept? [_ _]
    true)
  (proj/close [this]
    (.close (:writer this))
    (.close (:zip this))
    (.close (:file this))))

(defn json-writer-projector [config]
  (let [persistence (or (:persistence config) (pers/make-cached-jdbc-processor-persistence config))
        filename (or (:filename config) "D:\\test.json")]
    (->JsonWriterProjector (partial pers/path persistence) filename)))
