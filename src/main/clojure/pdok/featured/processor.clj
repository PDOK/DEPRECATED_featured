(ns pdok.featured.processor
  (:refer-clojure :exclude [flatten])
  (:require [pdok.featured.feature :as feature]
            [pdok.featured.persistence :as pers]
            [pdok.featured.generator :refer [random-json-feature-stream]]
            [pdok.featured.json-reader :refer :all]
            [pdok.featured.projectors :as proj]
            [clj-time [local :as tl]]
            [environ.core :refer [env]])
  (:import  [pdok.featured.projectors GeoserverProjector]))

(def ^:private pdok-fields [:action :id :dataset :collection :validity :geometry :current-validity
                            :parent-id :parent-collection :attributes])

(def ^:private processor-db {:subprotocol "postgresql"
                     :subname (or (env :processor-database-url) "//localhost:5432/pdok")
                     :user (or (env :processor-database-user) "postgres")
                     :password (or (env :processor-database-password) "postgres")})

(declare consume process pre-process)

(defn- make-invalid [feature reason]
  (let [current-reasons (or (:invalid-reasons feature) [])
        new-reasons (conj current-reasons reason)]
    (-> feature (assoc :invalid true) (assoc :invalid-reasons new-reasons))))

(defn- apply-all-features-validation [_ feature]
  (let [{:keys [dataset collection id validity geometry attributes]} feature]
    (if (some nil? [dataset collection id validity])
      (make-invalid feature "All feature require: dataset collection id validity")
      feature)))

(defn- apply-new-feature-requires-geometry-validation [_ feature]
  (if (nil? (:geometry feature))
    (make-invalid feature "New feature requires: geometry")
    feature))

(defn- apply-new-feature-requires-non-existing-stream-validation [persistence feature]
  (let [{:keys [dataset collection id]} feature]
    (if (and (not (:invalid feature)) (pers/stream-exists? persistence dataset collection id))
      (make-invalid feature (str "Stream already exists: " dataset ", " collection ", " id))
      feature))
  )

(defn- apply-non-new-feature-requires-existing-stream-validation [persistence feature]
  (let [{:keys [dataset collection id]} feature]
    (if-not (or (:invalid feature) (pers/stream-exists? persistence dataset collection id))
      (make-invalid feature (str "Stream does not exist yet: " dataset ", " collection ", " id))
      feature)))

(defn- apply-non-new-feature-requires-matching-current-validity-validation [persistence feature]
  (let [{:keys [dataset collection id validity current-validity]} feature]
    (if-not current-validity
      (make-invalid feature "Non new feature requires: current-validity")
      (let [stream-validity (pers/current-validity persistence dataset collection id)]
        (if (not= current-validity stream-validity)
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
    (when-not (:invalid validated)
      (let [{:keys [dataset collection id validity geometry attributes]} validated]
        (pers/create-stream persistence dataset collection id (:parent-collection feature) (:parent-id feature))
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
    (when-not (:invalid validated)
      (let [{:keys [dataset collection id current-validity validity geometry attributes]} validated]
        (doseq [p projectors] (proj/change-feature p validated))))
    validated))

(defn- process-nested-change-feature [processor feature]
  (process-change-feature processor (assoc feature :action :change)))

(defn- process-close-feature [{:keys [persistence projectors]} feature]
  (let [validated (->> feature
                       (apply-all-features-validation persistence)
                       (apply-closed-feature-cannot-be-changed-validation persistence)
                       (apply-change-feature-cannot-have-nil-geometry-validation persistence)
                       (apply-non-new-feature-requires-existing-stream-validation persistence)
                       (apply-non-new-feature-requires-matching-current-validity-validation persistence))]
    (when-not (:invalid validated)
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
    ;; niet echt mooi om hier append to stream te doen. Maar voor nu maar even wel.
    (pers/append-to-stream (:persistence processor) action dataset collection id validity geometry attributes)
    (process-close-feature processor no-update-nw)))

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
        parent-attributes (prefixed-attributes child-collection-key new-attributes)
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

(defn- close-all [processor meta-record]
  (let [{:keys [dataset collection parent-collection parent-id end-time]} meta-record
        persistence (:persistence processor)
        ids (pers/childs persistence dataset parent-collection parent-id collection )]
    (doseq [id ids]
      (let [validity (pers/current-validity persistence dataset collection id)
            state (pers/last-action persistence dataset collection id)]
        (when-not (= state :close)
          (consume processor {
                              :action :close
                              :dataset dataset
                              :collection collection
                              :id id
                              :current-validity validity
                              :validity end-time}))))))

(defn process [processor feature]
  "Processes feature event. Should return the feature, possibly with added data"
  (condp = (:action feature)
    :new (process-new-feature processor feature)
    :change (process-change-feature processor feature)
    :close (process-close-feature processor feature)
    :nested-new (process-nested-new-feature processor feature)
    :nested-change (process-nested-new-feature processor feature)
    :nested-close  (process-nested-close-feature processor feature)
    :close-all (close-all processor feature);; should save this too... So we can backtrack actions. Right?
    :drop nil ;; Not sure if we need the drop at all?
    (make-invalid feature "Unknown action")))

(defn pre-process [feature]
  (flatten (collect-attributes feature)))

(defmulti consume (fn [_ features] (type features)))

(defmethod consume clojure.lang.IPersistentMap [processor feature]
  (consume processor (list feature)))

(defmethod consume clojure.lang.ISeq [processor features]
  (let [pre-processed (mapcat pre-process features)]
    (doseq [f pre-processed]
      (when-let [result (process processor f)]
        (let [{:keys [action dataset collection id validity geometry attributes]} result]
          (pers/append-to-stream (:persistence processor) action dataset collection id validity geometry attributes))))))

(defn shutdown [{:keys [persistence projectors]}]
  "Shutdown feature store. Make sure all data is processed and persisted"
  (if-not persistence "persistence needed")
  (pers/close persistence)
  (doseq [p projectors] (proj/close p)))

(defn processor
  ([projectors]
   (let [jdbc-persistence (pers/cached-jdbc-processor-persistence {:db-config processor-db :batch-size 10000})]
    (processor jdbc-persistence projectors)))
  ([persistence projectors]
   (pers/init persistence)
   {:persistence persistence
    :projectors projectors}))

(defn performance-test [count & args]
  (with-open [json (apply random-json-feature-stream "perftest" "col1" count args)]
    (let [processor (processor [(proj/geoserver-projector {:db-config proj/data-db})])
          features (features-from-stream json)]
      (time (do (consume processor features)
                (shutdown processor)
                ))
      )))


; features-from-stream
