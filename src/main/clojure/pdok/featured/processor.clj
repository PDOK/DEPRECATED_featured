(ns pdok.featured.processor
  (:refer-clojure :exclude [flatten])
  (:require [pdok.random :as random]
            [pdok.featured.feature :as feature]
            [pdok.featured.persistence :as pers]
            [pdok.featured.json-reader :refer :all]
            [pdok.featured.projectors :as proj]
            [clj-time [core :as t] [local :as tl] [coerce :as tc]]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import  [pdok.featured.projectors GeoserverProjector]))

(def ^:private pdok-fields [:action :id :dataset :collection :validity :version :geometry :current-validity
                            :parent-id :parent-collection :parent-field :attributes :src])

(declare consume process pre-process append-feature)

(defn- make-invalid [feature reason]
  (let [current-reasons (or (:invalid-reasons feature) [])
        new-reasons (conj current-reasons reason)]
    (-> feature (assoc :invalid? true) (assoc :invalid-reasons new-reasons))))

(defn- apply-all-features-validation [_ feature]
  (let [{:keys [dataset collection id action validity geometry attributes]} feature]
    (if (or (some str/blank? [dataset collection id]) (and (not= action :delete) (nil? validity)))
      (make-invalid feature "All feature require: dataset collection id validity")
      feature)))

(defn- stream-exists? [persistence {:keys [dataset collection id]}]
  (and (pers/stream-exists? persistence dataset collection id)
       (not= :delete (pers/last-action persistence dataset collection id))))

(defn- apply-new-feature-requires-non-existing-stream-validation [persistence feature]
  (let [{:keys [dataset collection id]} feature]
    (if (and (not (:invalid? feature))
             (stream-exists? persistence feature))
      (make-invalid feature (str "Stream already exists: " dataset ", " collection ", " id))
      feature))
  )

(defn- apply-non-new-feature-requires-existing-stream-validation [persistence feature]
  (let [{:keys [dataset collection id]} feature]
    (if (and  (not (:invalid? feature))
              (not (stream-exists? persistence feature)))
      (make-invalid feature (str "Stream does not exist yet: " dataset ", " collection ", " id))
      feature)))

(defn- apply-non-new-feature-current-validity<=validity-validation [feature]
  (let [{:keys [validity current-validity]} feature]
    (if (and validity (t/before? validity current-validity))
      (make-invalid feature "Validity should >= current-validity")
      feature)))

(defn- apply-non-new-feature-current-validity-validation [persistence feature]
  (let [{:keys [dataset collection id validity current-validity]} feature]
    (if-not current-validity
      (make-invalid feature "Non new feature requires: current-validity")
      (let [stream-validity (pers/current-validity persistence dataset collection id)]
        (if  (not= current-validity stream-validity)
          (make-invalid feature
                        (str  "When updating current-validity should match" current-validity " != " stream-validity))
          (apply-non-new-feature-current-validity<=validity-validation feature))))))

(defn- apply-closed-feature-cannot-be-changed-validation [persistence feature]
  (let [{:keys [dataset collection id]} feature
        last-action (pers/last-action persistence dataset collection id)]
    (if (= :close last-action)
      (make-invalid feature "Closed features cannot be altered")
      feature))
  )

(defn- validate [{:keys [persistence]} feature]
  "Validates features. Nested actions are validated against new checks, because we always close the old nested features."
  (condp contains? (:action feature)
    #{:new :nested-new :nested-change :nested-close}
    (let [validated  (->> feature
                          (apply-all-features-validation persistence)
                          (apply-new-feature-requires-non-existing-stream-validation persistence))]
      validated)
    #{:change}
    (->> feature
         (apply-all-features-validation persistence)
         (apply-non-new-feature-requires-existing-stream-validation persistence)
         (apply-closed-feature-cannot-be-changed-validation persistence)
         (apply-non-new-feature-current-validity-validation persistence))
    #{:close}
    (->> feature
         (apply-all-features-validation persistence)
         (apply-closed-feature-cannot-be-changed-validation persistence)
         (apply-non-new-feature-requires-existing-stream-validation persistence)
         (apply-non-new-feature-current-validity-validation persistence))
    #{:delete}
    (->> feature
         (apply-all-features-validation persistence)
         (apply-non-new-feature-requires-existing-stream-validation persistence)
         (apply-non-new-feature-current-validity-validation persistence))
    feature
    ))

(defn- with-current-version [persistence feature]
  (if-not (:invalid? feature)
    (let [{:keys [dataset collection id]} feature]
      (assoc feature :current-version (pers/current-version persistence dataset collection id)))
    feature))

(defn- process-new-feature [{:keys [persistence projectors]} feature]
  (let [{:keys [dataset collection id validity geometry attributes]} feature]
      (pers/create-stream persistence dataset collection id
                          (:parent-collection feature) (:parent-id feature) (:parent-field feature))
      (append-feature persistence feature)
      (doseq [p projectors] (proj/new-feature p feature)))
  feature)

(defn- process-nested-new-feature [processor feature]
  (process-new-feature processor (assoc feature :action :new)))

(defn- process-change-feature [{:keys [persistence projectors]} feature]
  (let [enriched-feature (->> feature
                                  (with-current-version persistence))]
    (append-feature persistence enriched-feature)
    (doseq [p projectors] (proj/change-feature p enriched-feature))
    enriched-feature))

(defn- process-nested-change-feature [processor feature]
  "Nested change is the same a nested new"
  (process-new-feature processor (assoc feature :action :change)))

(defn- process-close-feature [{:keys [persistence projectors] :as processor} feature]
  (let [change-before-close (when-not (empty? (:attributes feature))
                               (process-change-feature processor 
                                                       (-> feature
                                                           (transient) 
                                                           (assoc! :action :change)
                                                           (dissoc! :src)
                                                           (assoc! :version (random/UUID))
                                                           (persistent!))))
        enriched-feature (->> feature 
                              (with-current-version persistence))]
    (append-feature persistence enriched-feature)
    (doseq [p projectors] (proj/close-feature p enriched-feature))
    (if change-before-close
      (list change-before-close enriched-feature)
      (list enriched-feature))))

(defn- process-nested-close-feature [processor feature]
  (let [nw (process-new-feature processor (assoc feature :action :new))
        {:keys [action dataset collection id validity geometry attributes]} nw
        no-update-nw (-> nw
                         (assoc :action :close)
                         (dissoc :geometry)
                         (assoc :attributes {})
                         (assoc :current-validity validity))]
    (list nw (process-close-feature processor no-update-nw))))

(defn- process-delete-feature [{:keys [persistence projectors]} feature]
  (let [enriched-feature (->> feature
                       (with-current-version persistence))]
    (append-feature persistence enriched-feature)
    (doseq [p projectors] (proj/delete-feature p enriched-feature))
    enriched-feature))

(defn- nested-features [attributes]
  (letfn [( flat-multi [[key values]] (map #(vector key %) values))]
    (let [single-features (filter #(map? (second %)) attributes)
          multi-features (filter #(sequential? (second %)) attributes)]
      (concat single-features (mapcat flat-multi multi-features)))))

(defn nested-action [action]
  (if-not action
    nil
    (if (.startsWith (name action) "nested-")
      action
      (keyword (str "nested-" (name action))))))

(defn- link-parent [[child-collection-key child] parent]
  (let [{:keys [dataset collection action id validity]} parent
        child-id (str (java.util.UUID/randomUUID))
        with-parent (-> (transient child)
                  (assoc! :dataset dataset)
                  (assoc! :parent-collection collection)
                  (assoc! :parent-id id)
                  (assoc! :parent-field (name child-collection-key))
                  (assoc! :action  (nested-action (:action parent)))
                  (assoc! :id child-id)
                  (assoc! :validity validity)
                  (assoc! :collection (str collection "$" (name child-collection-key))))]
    (persistent! with-parent)))

(defn- meta-delete-childs [{:keys [dataset collection id]}]
  {:action :delete-childs
   :dataset dataset
   :parent-collection collection
   :parent-id id})

(defn- meta-close-childs [features]
  "{:action :close-childs :dataset _ :collection _ :parent-collection _ :parent-id _ :validity _"
  (let [grouped (group-by #(select-keys % [:dataset :collection :parent-collection :parent-id :validity]) features)]
    (letfn [(meta [dataset collection parent-collection parent-id validity]
              {:action :close-childs
               :dataset dataset
               :collection collection
               :parent-collection parent-collection
               :parent-id parent-id
               :validity validity})]
      (map (fn [[{:keys [dataset collection parent-collection parent-id validity]} _]]
             (meta dataset collection parent-collection parent-id validity)) grouped)
      )))

(defn- flatten [processor feature]
  (if (= :delete (:action feature))
    (list feature (meta-delete-childs feature))
    (let [attributes (:attributes feature)
          nested (nested-features attributes)
          without-nested (apply dissoc attributes (map #(first %) nested))
          flat (assoc feature :attributes without-nested)
          linked-nested (map #(link-parent % feature) nested)
          meta-close-childs (meta-close-childs linked-nested)]
      (if (empty? linked-nested)
        (list flat)
        (cons flat (concat meta-close-childs (mapcat (partial pre-process processor) linked-nested))))
      ))
  )

(defn- attributes [obj]
  (apply dissoc obj pdok-fields)
  )

(defn- collect-attributes [feature]
  "Returns a feature where the attributes are collected in :attributes"
  (let [collected (merge (:attributes feature) (attributes feature))
        no-attributes (select-keys feature pdok-fields)
        collected (assoc no-attributes :attributes collected)]
    collected))

(defn- close-child [processor dataset collection id close-time]
  (let [persistence (:persistence processor)
        childs-of-child (pers/childs persistence dataset collection id)
        metas (map (fn [[child-col child-id]] {:action :close-childs
                                  :dataset dataset
                                  :collection child-col
                                  :parent-collection collection
                                  :parent-id id
                                  :validity close-time}) childs-of-child)
        validity (pers/current-validity persistence dataset collection id)
        state (pers/last-action persistence dataset collection id)]
        (if-not (= state :close)
          (consume processor (conj metas {:action :close
                                            :dataset dataset
                                            :collection collection
                                            :id id
                                            :current-validity validity
                                            :validity close-time}))
          (consume processor metas))))

(defn- close-childs [processor meta-record]
  (let [{:keys [dataset collection parent-collection parent-id validity]} meta-record
        persistence (:persistence processor)
        ids (pers/childs persistence dataset parent-collection parent-id collection)
        closed (doall (mapcat #(close-child processor dataset collection % validity) ids))]
     closed))

(defn- delete-child* [processor dataset collection id]
  (let [persistence (:persistence processor)
        validity (pers/current-validity persistence dataset collection id)]
    (consume processor (list
                        {:action :delete-childs
                         :dataset dataset
                         :parent-collection collection
                         :parent-id id}
                        {:action :delete
                         :dataset dataset
                         :collection collection
                         :id id
                         :current-validity validity}))))

(defn- delete-childs [processor {:keys [dataset parent-collection parent-id]}]
  (let [persistence (:persistence processor)
        ids (pers/childs persistence dataset parent-collection parent-id)
        deleted (doall (mapcat (fn [[col id]] (delete-child* processor dataset col id)) ids))]
    deleted))

(defn make-seq [obj]
  (if (seq? obj) obj (list obj)))

(defn process [processor feature]
  "Processes feature event. Should return the feature, possibly with added data"
  (if (:invalid? feature)
    feature
    (let [vf (assoc feature :version (random/UUID))
          processed
          (condp = (:action vf)
            :new (process-new-feature processor vf)
            :change (process-change-feature processor vf)
            :close (process-close-feature processor vf)
            :delete (process-delete-feature processor vf)
            :nested-new (process-nested-new-feature processor vf)
            :nested-change (process-nested-new-feature processor vf)
            :nested-close  (process-nested-close-feature processor vf)
            :close-childs (close-childs processor vf);; should save this too... So we can backtrack actions. Right?
            :delete-childs (delete-childs processor vf)
            (make-invalid vf (str "Unknown action:" (:action vf))))]
      processed)))


(defn rename-keys [src-map change-key]
  "Change keys in map with function change-key"
  (let [kmap (into {} (map #(vector %1 (change-key %1)) (keys src-map)))]
     (clojure.set/rename-keys src-map kmap)))

(defn lower-case [feature]
  (let [attributes-lower-case (rename-keys (:attributes feature) clojure.string/lower-case)
        feature-lower-case (assoc feature :attributes attributes-lower-case)]
    (cond-> feature-lower-case
            ((complement str/blank?) (:collection feature-lower-case))
            (update-in [:collection] str/lower-case)
            ((complement str/blank?) (:dataset feature-lower-case))
            (update-in [:dataset] str/lower-case))))

(defn pre-process [processor feature]
  (let [validated ((comp (partial validate processor) lower-case collect-attributes) feature)]
    (if (:invalid? validated)
      (vector validated)
      (flatten processor validated))))

(defmulti consume (fn [_ features] (type features)))

(defn- append-feature [persistence feature]
  (let [{:keys [version action dataset collection id validity geometry attributes]} feature]
    (pers/append-to-stream persistence version action dataset collection id validity geometry attributes)
    feature))

(defn- update-statistics [{:keys [statistics]} feature]
  (when statistics
    (swap! statistics update :n-processed inc)
    (when (:src feature) (swap! statistics update :n-src inc))
    (when (:invalid? feature)
      (swap! statistics update :n-errored inc)
      (swap! statistics update :errored #(conj % (:id feature)))))
  )

(defmethod consume clojure.lang.IPersistentMap [processor feature]
  (let [pre-processed (pre-process processor feature)
        consumed (filter (complement nil?) (mapcat #(make-seq (process processor %)) pre-processed))]
    (doseq [f consumed]
      (if (:invalid? f) (log/warn (:id f) (:invalid-reasons f)))
      (update-statistics processor f))
    consumed))

(defmethod consume clojure.lang.ISeq [processor features]
  (mapcat #(consume processor %) features))

(defn shutdown [{:keys [persistence projectors statistics]}]
  "Shutdown feature store. Make sure all data is processed and persisted"
  (let [closed-persistence (pers/close persistence)
        closed-projectors (doall (map proj/close projectors))
        _ (when statistics (log/info @statistics))]
    {:persistence closed-persistence
     :projectors closed-projectors
     :statistics (if statistics @statistics {})}))

(defn add-projector [processor projector]
  (let [initialized-projector (proj/init projector)]
    (update-in processor [:projectors] conj initialized-projector)))

(defn create
  ([persistence] (create persistence []))
  ([persistence & projectors]
   (let [initialized-persistence (pers/init persistence)
         initialized-projectors (doall (map proj/init (clojure.core/flatten projectors)))]
     {:persistence initialized-persistence
      :projectors initialized-projectors
      :statistics (atom {:n-src 0 :n-processed 0 :n-errored 0 :errored '()})})))

; features-from-stream
