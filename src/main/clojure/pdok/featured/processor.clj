(ns pdok.featured.processor
  (:refer-clojure :exclude [flatten])
  (:require [pdok.random :as random]
            [pdok.featured.feature :as feature]
            [pdok.featured.persistence :as pers]
            [pdok.featured.json-reader :refer :all]
            [pdok.featured.projectors :as proj]
            [clj-time [local :as tl] [coerce :as tc]]
            [environ.core :refer [env]]
            [clojure.string :as str])
  (:import  [pdok.featured.projectors GeoserverProjector]))

(def ^:private pdok-fields [:action :id :dataset :collection :validity :version :geometry :current-validity
                            :parent-id :parent-collection :attributes])

(declare consume process pre-process append-feature)

(defn- make-invalid [feature reason]
  (let [current-reasons (or (:invalid-reasons feature) [])
        new-reasons (conj current-reasons reason)]
    (-> feature (assoc :invalid? true) (assoc :invalid-reasons new-reasons))))

(defn- apply-all-features-validation [_ feature]
  (let [{:keys [dataset collection id validity geometry attributes]} feature]
    (if (or (some str/blank? [dataset collection id]) (nil? validity))
      (make-invalid feature "All feature require: dataset collection id validity")
      feature)))

(defn- apply-new-feature-requires-geometry-validation [_ feature]
  (if (nil? (:geometry feature))
    (make-invalid feature "New feature requires: geometry")
    feature))

(defn- apply-new-feature-requires-non-existing-stream-validation [persistence feature]
  (let [{:keys [dataset collection id]} feature]
    (if (and (not (:invalid? feature)) (pers/stream-exists? persistence dataset collection id))
      (make-invalid feature (str "Stream already exists: " dataset ", " collection ", " id))
      feature))
  )

(defn- apply-non-new-feature-requires-existing-stream-validation [persistence feature]
  (let [{:keys [dataset collection id]} feature]
    (if-not (or (:invalid? feature) (pers/stream-exists? persistence dataset collection id))
      (make-invalid feature (str "Stream does not exist yet: " dataset ", " collection ", " id))
      feature)))

(defn- apply-non-new-feature-requires-matching-current-validity-validation [persistence feature]
  (let [{:keys [dataset collection id validity current-validity]} feature]
    (if-not current-validity
      (make-invalid feature "Non new feature requires: current-validity")
      (let [stream-validity (pers/current-validity persistence dataset collection id)]
        (if  (not= (tc/to-date current-validity) (tc/to-date stream-validity))
          (make-invalid feature "When updating current-validity should match")
          feature)))))

(defn- apply-change-feature-cannot-have-nil-geometry-validation [_ feature]
  (let [geometry (:geometry feature)]
    (if (and (contains? feature :geometry) (nil? geometry))
      (make-invalid feature "Change feature cannot have nil geometry (also remove the key)")
      feature)))

(defn- apply-closed-feature-cannot-be-changed-validation [persistence feature]
  (let [{:keys [dataset collection id]} feature
        last-action (pers/last-action persistence dataset collection id)]
    (if (= :close last-action)
      (make-invalid feature "Closed features cannot be altered")
      feature))
  )

(defn- process-new-feature [{:keys [persistence projectors]} feature]
  (let [validated (->> feature
                       (apply-all-features-validation persistence)
                       (apply-new-feature-requires-geometry-validation persistence)
                       (apply-new-feature-requires-non-existing-stream-validation persistence))]
    (when-not (:invalid? validated)
      (let [{:keys [dataset collection id validity geometry attributes]} validated]
        (pers/create-stream persistence dataset collection id (:parent-collection feature) (:parent-id feature))
        (append-feature persistence validated)
        (doseq [p projectors] (proj/new-feature p validated))))
    validated))

(defn- process-nested-new-feature [processor feature]
  (process-new-feature processor (assoc feature :action :new)))

(defn- process-change-feature [{:keys [persistence projectors]} feature]
  (let [validated (->> feature
                       (apply-all-features-validation persistence)
                       (apply-change-feature-cannot-have-nil-geometry-validation persistence)
                       (apply-non-new-feature-requires-existing-stream-validation persistence)
                       (apply-closed-feature-cannot-be-changed-validation persistence)
                       (apply-non-new-feature-requires-matching-current-validity-validation persistence))]
    (when-not (:invalid? validated)
      (append-feature persistence validated)
      (let [{:keys [dataset collection id current-validity validity geometry attributes]} validated]
        (doseq [p projectors] (proj/change-feature p validated))))
    validated))

(defn- process-nested-change-feature [processor feature]
  "Nested change is the same a nested new"
  (process-new-feature processor (assoc feature :action :change)))

(defn- process-close-feature [{:keys [persistence projectors]} feature]
  (let [validated (->> feature
                       (apply-all-features-validation persistence)
                       (apply-closed-feature-cannot-be-changed-validation persistence)
                       (apply-change-feature-cannot-have-nil-geometry-validation persistence)
                       (apply-non-new-feature-requires-existing-stream-validation persistence)
                       (apply-non-new-feature-requires-matching-current-validity-validation persistence))]
    (when-not (:invalid? validated)
      (append-feature persistence validated)
      (let [{:keys [dataset collection id current-validity validity geometry attributes]} validated]
        (doseq [p projectors] (proj/close-feature p validated))))
    validated))

(defn- process-nested-close-feature [processor feature]
  (let [nw (process-new-feature processor (assoc feature :action :new))
        {:keys [action dataset collection id validity geometry attributes]} nw
        no-update-nw (-> nw
                         (assoc :action :close)
                         (dissoc :geometry)
                         (assoc :attributes [])
                         (assoc :current-validity validity))]
    (list nw (process-close-feature processor no-update-nw))))

(defn- nested-features [attributes]
  (letfn [( flat-multi [[key values]] (map #(vector key %) values))]
    (let [single-features (filter #(map? (second %)) attributes)
          multi-features (filter #(sequential? (second %)) attributes)]
      (concat single-features (mapcat flat-multi multi-features)))))

(defn- link-parent [[child-collection-key child] parent]
  (let [{:keys [dataset collection action id validity]} parent
        child-id (str (java.util.UUID/randomUUID))
        with-parent (-> (transient child)
                  (assoc! :dataset dataset)
                  (assoc! :parent-collection collection)
                  (assoc! :parent-id id)
                  (assoc! :action  (keyword (str "nested-" (name (:action parent)))))
                  (assoc! :id child-id)
                  (assoc! :validity validity)
                  (assoc! :collection (str collection "$" (name child-collection-key))))]
    (persistent! with-parent)))

(defn- prefixed-attributes [prefix attributes]
  (let [replacement-map (persistent! (reduce (fn [c [k v]] (assoc! c k v)) (transient {})
                                             (map #(vector %1 (str prefix "$" (name %1)) ) (keys attributes))))
        prefixed (clojure.set/rename-keys attributes replacement-map)]
    prefixed))

(defn- enrich [[child-collection-key child] parent new-attributes]
  (let [{:keys [dataset collection action id validity]} parent
        child-id (str (java.util.UUID/randomUUID))
        parent-attributes (prefixed-attributes "_parent" new-attributes)
        enriched (-> (transient child)
                     (assoc! :dataset dataset)
                     (assoc! :parent-collection collection)
                     (assoc! :parent-id id)
                     (assoc! :collection (str collection "$" (name child-collection-key)))
                     (assoc! :action (keyword (str "nested-" (name (:action parent)))))
                     (assoc! :id child-id)
                     (assoc! :validity validity)
                     (assoc! :attributes (merge (:attributes child) parent-attributes)))]
   (persistent! enriched) ))

(defn- meta-close-all-features [features]
  "{:action :close-all :dataset _ :collection _ :parent-collection _ :parent-id _ :end-time _"
  (let [grouped (group-by #(select-keys % [:dataset :collection :parent-collection :parent-id :validity]) features)]
    (letfn [(meta [dataset collection parent-collection parent-id validity]
              {:action :close-all
               :dataset dataset
               :collection collection
               :parent-collection parent-collection
               :parent-id parent-id
               :end-time validity})]
      (map (fn [[{:keys [dataset collection parent-collection parent-id validity]} _]]
             (meta dataset collection parent-collection parent-id validity)) grouped)
      )))

(defn- flatten-with-geometry [feature]
  (let [attributes (:attributes feature)
        nested (nested-features attributes)
        without-nested (apply dissoc attributes (map #(first %) nested))
        flat (assoc feature :attributes without-nested)
        linked-nested (map #(link-parent % feature) nested)
        close-all (meta-close-all-features linked-nested)]
    (if (empty? linked-nested)
      (list  flat)
      (cons flat (concat close-all (mapcat pre-process linked-nested))))
    )
  )

(defn- flatten-without-geometry [feature]
  ;; collection van parent is prefix, en voeg parents toe aan childs
  (let [dropped-parent (assoc feature :action :drop)
        attributes (:attributes feature)
        nested (nested-features attributes)
        attributes-without-nested (apply dissoc attributes (map #(first %) nested))
        enriched-nested (map #(enrich % feature attributes-without-nested) nested)
        close-all (meta-close-all-features enriched-nested)]
    (if (empty? enriched-nested)
      (list (if (= (:action feature) :new) dropped-parent feature))
      (cons dropped-parent (concat close-all (mapcat pre-process enriched-nested)))))
  )

(defn- flatten [feature]
  (if (contains? feature :geometry)
    (flatten-with-geometry feature)
    (flatten-without-geometry feature)))

(defn- attributes [obj]
  (apply dissoc obj pdok-fields)
  )

(defn- collect-attributes [feature]
  "Returns a feature where the attributes are collected in :attributes"
  (let [collected (merge (:attributes feature) (attributes feature))
        no-attributes (select-keys feature pdok-fields)
        collected (assoc no-attributes :attributes collected)]
    collected))

(defn- close-all* [processor dataset collection id end-time]
  (let [persistence (:persistence processor)
        validity (pers/current-validity persistence dataset collection id)
            state (pers/last-action persistence dataset collection id)]
        (when-not (= state :close)
          (consume processor {:action :close
                              :dataset dataset
                              :collection collection
                              :id id
                              :current-validity validity
                              :validity end-time}))))

(defn- close-all [processor meta-record]
  (let [{:keys [dataset collection parent-collection parent-id end-time]} meta-record
        persistence (:persistence processor)
        ids (pers/childs persistence dataset parent-collection parent-id collection )
        closed (doall (mapcat #(close-all* processor dataset collection % end-time) ids))]
    closed))

(defn make-seq [obj]
  (if (seq? obj) obj (list obj)))

(defn process [processor feature]
  "Processes feature event. Should return the feature, possibly with added data"
  (let [vf (assoc feature :version (random/UUID))]
    (condp = (:action vf)
      :new (process-new-feature processor vf)
      :change (process-change-feature processor vf)
      :close (process-close-feature processor vf)
      :nested-new (process-nested-new-feature processor vf)
      :nested-change (process-nested-new-feature processor vf)
      :nested-close  (process-nested-close-feature processor vf)
      :close-all (close-all processor vf);; should save this too... So we can backtrack actions. Right?
      :drop nil ;; Not sure if we need the drop at all?
      (make-invalid vf "Unknown action"))))


(defn rename-keys [src-map change-key]
  "Change keys in map with function change-key"
  (let [kmap (into {} (map #(vector %1 (change-key %1)) (keys src-map)))]
     (clojure.set/rename-keys src-map kmap)))

(defn lower-case [feature]
  (let [attributes-lower-case (rename-keys (:attributes feature) clojure.string/lower-case)
        feature-lower-case (assoc feature :attributes attributes-lower-case)]
    (if (str/blank? (:collection feature-lower-case))
      feature-lower-case
      (update-in feature-lower-case [:collection] str/lower-case))))

(defn pre-process [feature]
   ((comp flatten lower-case collect-attributes) feature))

(defmulti consume (fn [_ features] (type features)))

(defn- append-feature [persistence feature]
  (let [{:keys [version action dataset collection id validity geometry attributes]} feature]
    (pers/append-to-stream persistence version action dataset collection id validity geometry attributes)
    feature))

(defmethod consume clojure.lang.IPersistentMap [processor feature]
  (let [pre-processed (pre-process feature)
        consumed (filter (complement nil?) (mapcat #(make-seq (process processor %)) pre-processed))]
    consumed))

(defmethod consume clojure.lang.ISeq [processor features]
  (mapcat #(consume processor %) features))

(defn shutdown [{:keys [persistence projectors]}]
  "Shutdown feature store. Make sure all data is processed and persisted"
  (let [closed-persistence (pers/close persistence)
        closed-projectors (doall (map proj/close projectors))]
    {:persistence closed-persistence
     :projectors closed-projectors}))

(defn processor
  ([persistence projectors]
   (let [initialized-persistence (pers/init persistence)
         initialized-projectors (doall (map proj/init projectors))]
     (doseq [p projectors]
       (proj/init p))
     {:persistence initialized-persistence
      :projectors initialized-projectors})))

; features-from-stream
