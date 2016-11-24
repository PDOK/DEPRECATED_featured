(ns pdok.featured.processor
  (:refer-clojure :exclude [flatten])
  (:require [pdok.random :as random]
            [pdok.util :refer [with-bench]]
            [pdok.featured.feature :as feature]
            [pdok.featured.persistence :as pers]
            [pdok.featured.json-reader :refer :all]
            [pdok.featured.projectors :as proj]
            [pdok.featured.config :as config]
            [clojure.core.async :as a]
            [clj-time [core :as t] [local :as tl] [coerce :as tc]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [pdok.featured.tiles :as tiles]))

(def ^:private pdok-fields [:action :id :dataset :collection :validity :version :geometry :current-validity
                            :parent-id :parent-collection :parent-field :attributes :src])

(declare consume consume* process pre-process append-feature)

(defn make-seq [obj]
  (if (seq? obj) obj (list obj)))

(defn- make-invalid [feature reason]
  (let [current-reasons (or (:invalid-reasons feature) [])
        new-reasons (conj current-reasons reason)]
    (-> feature (assoc :invalid? true) (assoc :invalid-reasons new-reasons))))

(defn- apply-all-features-validation [persistence {:keys [invalids]} feature]
  (let [{:keys [collection id action validity geometry attributes]} feature
        parent-set #{:parent-collection :parent-id}
        parent-check (map #(contains? feature %) parent-set)]
    (cond-> feature
      (or (some str/blank? [collection id]) (and (not= action :delete) (nil? validity)))
      (make-invalid "All feature require: collection id validity")
      (and (some true? parent-check) (not (every? true? parent-check)))
      (make-invalid "Feature should contain both parent-collection and parent-id or none")
      (and (every? true? parent-check) (not (pers/stream-exists? persistence (:parent-collection feature) (:parent-id feature))))
      (make-invalid "Parent should exist")
      (@invalids [(:collection feature) (:id feature)])
      (make-invalid "Feature marked invalid")
      (@invalids [(:parent-collection feature) (:parent-id feature)])
      (make-invalid "Parent marked invalid"))))

(defn- stream-exists? [persistence {:keys [collection id]}]
  (and (pers/stream-exists? persistence collection id)
       (not= :delete (pers/last-action persistence collection id))))

(defn- apply-new-feature-requires-non-existing-stream-validation [persistence feature]
  (let [{:keys [collection id]} feature]
    (if (and (not (:invalid? feature))
             (stream-exists? persistence feature))
      (make-invalid feature (str "Stream already exists: " collection ", " id))
      feature))
  )

(defn- apply-non-new-feature-requires-existing-stream-validation [persistence feature]
  (let [{:keys [collection id]} feature]
    (if (and  (not (:invalid? feature))
              (not (stream-exists? persistence feature)))
      (make-invalid feature (str "Stream does not exist yet: " collection ", " id))
      feature)))

(defn- apply-non-new-feature-current-validity<=validity-validation [feature]
  (let [{:keys [validity current-validity]} feature]
    (if (and validity (t/before? validity current-validity))
      (make-invalid feature "Validity should >= current-validity")
      feature)))

(defn- apply-non-new-feature-current-validity-validation [persistence feature]
  (let [{:keys [collection id validity current-validity]} feature]
    (if-not current-validity
      (make-invalid feature "Non new feature requires: current-validity")
      (let [stream-validity (pers/current-validity persistence collection id)]
        (if  (not= current-validity stream-validity)
          (make-invalid feature
                        (str  "When updating current-validity should match" current-validity " != " stream-validity))
          (apply-non-new-feature-current-validity<=validity-validation feature))))))

(defn- apply-closed-feature-cannot-be-changed-validation [persistence feature]
  (let [{:keys [collection id]} feature
        last-action (pers/last-action persistence collection id)]
    (if (= :close last-action)
      (make-invalid feature "Closed features cannot be altered")
      feature))
  )

(defn- validate [{:keys [persistence invalids] :as processor} feature]
  "Validates features. Nested actions are validated against new checks, because we always close the old nested features."
  (let [validated
        (condp contains? (:action feature)
          #{:new :nested-new :nested-change :nested-close}
          (->> feature
               (apply-all-features-validation persistence processor)
               (apply-new-feature-requires-non-existing-stream-validation persistence))
          #{:change}
          (->> feature
               (apply-all-features-validation persistence processor)
               (apply-non-new-feature-requires-existing-stream-validation persistence)
               (apply-closed-feature-cannot-be-changed-validation persistence)
               (apply-non-new-feature-current-validity-validation persistence))
          #{:new|change}
          (->> feature
               (apply-all-features-validation persistence processor)
               )
          #{:close}
          (->> feature
               (apply-all-features-validation persistence processor)
               (apply-closed-feature-cannot-be-changed-validation persistence)
               (apply-non-new-feature-requires-existing-stream-validation persistence)
               (apply-non-new-feature-current-validity-validation persistence))
          #{:delete}
          (cond->> feature
            true (apply-all-features-validation persistence processor)
            (:check-existence-on-delete processor)
            (apply-non-new-feature-requires-existing-stream-validation persistence)
            (:check-validity-on-delete processor)
            (apply-non-new-feature-current-validity-validation persistence))
          feature
          )]
    (when (:invalid? validated)
      (vswap! invalids conj [(:collection validated) (:id validated)]))
    validated))

(defn- with-current-version [persistence feature]
  (if-not (:invalid? feature)
    (let [{:keys [collection id]} feature]
      (assoc feature :current-version (pers/current-version persistence collection id)))
    feature))

(defn project! [processor proj-fn & more]
  (doseq [p (:projectors processor)]
    (apply proj-fn p more)))

(defn doto-projectors! [processor action-fn]
  (doseq [p (:projectors processor)]
    (action-fn p)))

(defn- process-new-feature [{:keys [persistence] :as processor} feature]
  (let [{:keys [collection id parent-collection]} feature]
    (when-not (pers/collection-exists? persistence collection parent-collection)
      (pers/create-collection persistence collection parent-collection)
      (project! processor proj/new-collection collection parent-collection))
    (when-not (pers/stream-exists? persistence collection id)
      (pers/create-stream persistence collection id
                          (:parent-collection feature) (:parent-id feature) (and (:parent-collection feature) (or (:parent-field feature) (:collection feature)))))
    (append-feature persistence feature)
    (project! processor proj/new-feature feature))
  feature)

(defn- process-nested-new-feature [processor feature]
  (process-new-feature processor (assoc feature :action :new)))

(defn- process-change-feature [{:keys [persistence] :as processor} feature]
  (let [enriched-feature (->> feature
                                  (with-current-version persistence))]
    (append-feature persistence enriched-feature)
    (project! processor proj/change-feature enriched-feature)
    enriched-feature))

(defn- process-new|change-feature [{:keys [persistence] :as processor} feature]
  (if (stream-exists? persistence feature)
    (process-change-feature processor (assoc feature :action :change))
    (process-new-feature processor (assoc feature :action :new))))

(defn- process-nested-change-feature [processor feature]
  "Nested change is the same a nested new"
  (process-new-feature processor (assoc feature :action :change)))

(defn- process-close-feature [{:keys [persistence] :as processor} feature]
  (if (empty? (:attributes feature))
    (let [enriched-feature (->> feature (with-current-version persistence))]
      (append-feature persistence enriched-feature)
      (project! processor proj/close-feature enriched-feature)
      (list enriched-feature))
    (let [change-feature (with-current-version persistence
                                               (-> feature
                                               (transient)
                                               (assoc! :action :change)
                                               (dissoc! :src)
                                               (assoc! :version (:version feature))
                                               (persistent!)))
          _ (process-change-feature processor change-feature)
          close-feature (with-current-version persistence (assoc feature :version (random/ordered-UUID)))]
      (append-feature persistence close-feature)
      (project! processor proj/close-feature close-feature)
      (list change-feature close-feature))))

(defn- process-nested-close-feature [processor feature]
  (let [nw (process-new-feature processor (assoc feature :action :new))
        {:keys [action collection id validity geometry attributes]} nw
        no-update-nw (-> nw (transient)
                         (assoc! :action :close)
                         (dissoc! :geometry)
                         (assoc! :attributes {})
                         (assoc! :current-validity validity)
                         (persistent!))]
    (list nw (process-close-feature processor no-update-nw))))

(defn- process-delete-feature [{:keys [persistence projectors] :as processor} feature]
  (let [enriched-feature (->> feature
                       (with-current-version persistence))]
    (append-feature persistence enriched-feature)
    (project! processor proj/delete-feature enriched-feature)
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
  (let [{:keys [collection action id validity]} parent
        child-id (str (java.util.UUID/randomUUID))
        with-parent (-> (transient child)
                  (assoc! :parent-collection collection)
                  (assoc! :parent-id id)
                  (assoc! :parent-field (name child-collection-key))
                  (assoc! :action  (nested-action (:action parent)))
                  (assoc! :id child-id)
                  (assoc! :validity validity)
                  (assoc! :collection (str collection "$" (name child-collection-key))))]
    (persistent! with-parent)))

(defn- meta-delete-childs [{:keys [collection id]}]
  {:action :delete-childs
   :parent-collection collection
   :parent-id id})

(defn- meta-close-childs [features]
  "{:action :close-childs _ :collection _ :parent-collection _ :parent-id _ :validity _"
  (distinct (map (fn [f] {:action :close-childs
                          :parent-collection (:parent-collection f)
                          :parent-id (:parent-id f)
                          :collection (:collection f)
                          :validity (:validity f)}) features)))

(defn- flatten [processor feature]
  (condp = (:action feature)
    :delete (list feature (meta-delete-childs feature))
    :close (list feature {:action :close-childs
                          :parent-collection (:collection feature)
                          :parent-id (:id feature)
                          :validity (:validity feature)})
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

(defn- close-child [processor parent-collection parent-id collection id close-time]
  (let [persistence (:persistence processor)
        childs-of-child (pers/childs persistence collection id)
        metas (map (fn [[child-col child-id]] {:action :close-childs
                                  :collection child-col
                                  :parent-collection collection
                                  :parent-id id
                                  :validity close-time}) childs-of-child)
        validity (pers/current-validity persistence collection id)
        state (pers/last-action persistence collection id)]
        (if-not (= state :close)
          (mapcat (partial process processor)
                  (conj metas {:action :close
                               :parent-collection parent-collection
                               :parent-id parent-id
                               :collection collection
                               :id id
                               :current-validity validity
                               :validity close-time}))
          (mapcat (partial process processor) metas))))

(defn- close-childs [processor meta-record]
  (let [{:keys [collection parent-collection parent-id validity]} meta-record
        persistence (:persistence processor)
        col-ids (if collection
                  (pers/childs persistence parent-collection parent-id collection)
                  (pers/childs persistence parent-collection parent-id))
        closed (doall (mapcat
                       (fn [[col id]] (close-child processor parent-collection parent-id
                                                col id validity)) col-ids))]
     closed))

(defn- delete-child* [processor collection id]
  (let [persistence (:persistence processor)
        validity (pers/current-validity persistence collection id)]
    (mapcat (partial process processor) (list
                                  {:action :delete-childs
                                   :parent-collection collection
                                   :parent-id id}
                                  {:action :delete
                                   :collection collection
                                   :id id
                                   :current-validity validity}))))

(defn- delete-childs [processor {:keys [parent-collection parent-id]}]
  (let [persistence (:persistence processor)
        ids (pers/childs persistence parent-collection parent-id)
        deleted (doall (mapcat (fn [[col id]] (delete-child* processor col id)) ids))]
    deleted))

(defn process [processor feature]
  "Processes feature events. Should return the feature, possibly with added data, returns sequence"
  (let [validated (if (:disable-validation processor) feature (validate processor feature))]
    (if (:invalid? validated)
      (make-seq validated)
      (let [vf (assoc validated :version (random/ordered-UUID))
            processed
            (condp = (:action vf)
              :new (process-new-feature processor vf)
              :change (process-change-feature processor vf)
              :new|change (process-new|change-feature processor vf)
              :close (process-close-feature processor vf)
              :delete (process-delete-feature processor vf)
              :nested-new (process-nested-new-feature processor vf)
              :nested-change (process-nested-new-feature processor vf)
              :nested-close  (process-nested-close-feature processor vf)
              :close-childs (close-childs processor vf);; should save this too... So we can backtrack actions. Right?
              :delete-childs (delete-childs processor vf)
              (make-invalid vf (str "Unknown action:" (:action vf))))]
        (make-seq processed)))))


(defn rename-keys [src-map change-key]
  "Change keys in map with function change-key"
  (let [kmap (into {} (map #(vector %1 (change-key %1)) (keys src-map)))]
     (clojure.set/rename-keys src-map kmap)))

(defn lower-case [feature]
  (let [attributes-lower-case (rename-keys (:attributes feature) clojure.string/lower-case)
        feature-lower-case (assoc feature :attributes attributes-lower-case)]
    (cond-> feature-lower-case
            ((complement str/blank?) (:collection feature-lower-case))
            (update-in [:collection] str/lower-case))))

(defn pre-process [processor feature]
  (let [prepped ((comp lower-case collect-attributes) feature)
        ]
    (flatten processor prepped)))

(defn- append-feature [persistence feature]
  (let [{:keys [version action collection id validity geometry attributes]} feature]
    (pers/append-to-stream persistence version action collection id validity geometry attributes)
    feature))

(defn- update-statistics [{:keys [statistics]} feature]
  (when statistics
    (swap! statistics update :n-processed inc)
    (when (:src feature) (swap! statistics update :n-src inc))
    (when (:geometry feature)
      (let [tiles (-> feature :geometry tiles/nl)]
        (swap! statistics update :updated-tiles #(clojure.set/union % tiles))))
    (when (:invalid? feature)
      (swap! statistics update :n-errored inc)
      (swap! statistics update :errored #(conj % (:id feature)))))
  )

(defn consume* [processor features]
  (letfn [(consumer [feature]
            (let [consumed (filter (complement nil?) (process processor feature))]
              (doseq [f consumed]
                (if (:invalid? f) (log/warn (:id f) (:invalid-reasons f)))
                (update-statistics processor f))
              consumed))]
    (mapcat consumer features)))

(defn consume-partition [processor features]
  (when (seq? features)
    (let [prepped (with-bench t (log/debug "Preprocessed batch in" t "ms")
                    (mapcat (partial pre-process processor) features))
          _ (pers/flush (:persistence processor)) ;; flush before run, to save cache in shutdown
          _ (pers/prepare (:persistence processor) prepped)
          consumed (with-bench t (log/debug "Consumed batch in" t "ms")
                     (doall (consume* processor prepped)))
          _ (doto-projectors! processor proj/flush)]
      consumed)))

(defn consume [processor features]
  (let [n (:batch-size processor)]
    (when (seq features)
      (concat (consume-partition processor (take n features))
              (lazy-seq (consume processor (drop n features)))))))

(defn replay [{:keys [persistence projectors batch-size statistics] :as processor}
              last-n root-collection]
  "Sends the last n events from persistence to the projectors"
  (let [collections (when root-collection (pers/collection-tree persistence root-collection))
        features (pers/get-last-n persistence last-n collections)
        parts (a/pipe features (a/chan 1 (partition-all batch-size)))]
    (loop [part (a/<!! parts)]
      (when (seq part)
        (let [enriched-part (pers/enrich-with-parent-info persistence part)]
          (pers/flush persistence)
          (pers/prepare persistence enriched-part)
          (doseq [f enriched-part]
            (condp = (:action f)
              :new (project! processor proj/new-feature f)
              :change (project! processor proj/change-feature f)
              :close (project! processor proj/close-feature f)
              :delete (project! processor proj/delete-feature f))
            (swap! statistics update :replayed inc))
          (doto-projectors! processor proj/flush)
          (recur (a/<!! parts)))))))

(defn shutdown [{:keys [persistence projectors statistics] :as processor}]
  "Shutdown feature store. Make sure all data is processed and persisted"
  (let [_ (doto-projectors! processor proj/close)
        ;; timeline uses persistence, close persistence after projectors
        closed-persistence (pers/close persistence)
        _ (when statistics (log/info @statistics))]
    {:persistence closed-persistence
     :projectors projectors
     :statistics (if statistics @statistics {})}))

(defn add-projector [processor projector]
  (let [initialized-projector (proj/init projector (:dataset processor) (pers/collections (:persistence processor)))]
    (update-in processor [:projectors] conj initialized-projector)))

(defn create
  ([dataset persistence] (create dataset persistence []))
  ([dataset persistence projectors] (create {} dataset persistence projectors))
  ([options dataset persistence projectors]
   {:pre [(map? options) (string? dataset)]}
   (let [initialized-persistence (pers/init persistence dataset)
         initialized-projectors (doall (map #(proj/init % dataset (pers/collections initialized-persistence))
                                            (clojure.core/flatten projectors)))
         batch-size (or (config/env :processor-batch-size) 10000)]
     (merge {:dataset dataset
             :check-validity-on-delete true
             :check-existence-on-delete true
             :disable-validation false
             :persistence initialized-persistence
             :projectors initialized-projectors
             :batch-size batch-size
             :invalids (volatile! #{})
             :statistics (atom {:n-src 0 :n-processed 0 :n-errored 0 :errored '() :replayed 0 :updated-tiles #{}})}
            options))))
