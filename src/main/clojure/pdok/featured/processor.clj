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

(defn- process-new-feature [{:keys [persistence projectors]} feature]
  ;;(println "hiero: " feature)
  (let [{:keys [dataset collection id validity geometry attributes]} feature]
    (if (some nil? [dataset collection id validity geometry])
      "New feature requires: dataset collection id validity geometry"
      (if (pers/stream-exists? persistence dataset collection id)
        (str "Stream already exists: " dataset ", " collection ", " id)
        (do (pers/create-stream persistence dataset collection id)
            (pers/append-to-stream persistence :new dataset collection id validity geometry attributes)
            (doseq [p projectors] (proj/new-feature p feature)))))))

(defn- process-change-feature [{:keys [persistence projectors]} feature]
  (let [{:keys [dataset collection id current-validity validity]} feature]
    (if (some nil? [dataset collection id current-validity validity])
      "Change feature requires dataset collection id current-validity validity"
      (let [geometry (:geometry feature)]
        (if (and (contains? feature :geometry) (nil? geometry))
          "Change feature cannot have nil geometry (also remove the key)"
          (if (not (pers/stream-exists? persistence dataset collection id))
            "Create feature stream first with :new"
            (let [stream-validity (pers/current-validity persistence dataset collection id)]
              (if (not= current-validity stream-validity)
                "When changing current-validity should match"
                (do (pers/append-to-stream persistence :change dataset collection
                                           id validity geometry (:attributes feature))
                    (doseq [p projectors] (proj/change-feature p feature)))))))))))



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
                  (assoc! :action action)
                  (assoc! :id child-id)
                  (assoc! :validity validity)
                  (assoc! :collection (str collection "$" (name child-collection-key)))
                  (assoc! :parent-id id))]
    (persistent! with-parent)))

(defn- combine-attributes [new-prefix new-attributes target-attributes]
  (let [replacement-map (persistent! (reduce (fn [c [k v]] (assoc! c k v)) (transient {})
                                             (map #(vector %1 (str new-prefix "$" (name %1)) ) (keys new-attributes))))
        prefixed-new (clojure.set/rename-keys new-attributes replacement-map)
        combined (persistent! (reduce (fn [c [k v]] (assoc! c k v)) (transient target-attributes) prefixed-new ))]))

(defn- enrich [[child-collection-key child] parent]
  (let [{:keys [dataset collection action id validity]} parent
        child-id (str id "$" (java.util.UUID/randomUUID))
        new-attributes (combine-attributes child-collection-key (:attributes parent) (:attributes child))
        enriched (-> (transient child)
                     (assoc! :dataset dataset)
                     (assoc! :collection (str collection "$" child-collection-key))
                     (assoc! :action action)
                     (assoc! :id child-id)
                     (assoc! :validity validity)
                     (assoc! :attributes new-attributes))]
    ))

(defn- flatten-with-geometry [feature]
  (let [attributes (:attributes feature)
        nested (nested-features attributes)
        without-nested (apply dissoc attributes (map #(first %) nested))
        flat (assoc feature :attributes without-nested)
        enriched-nested (map #(link-parent % feature) nested)]
    (if (empty? enriched-nested)
      flat
      (cons flat enriched-nested))
    )
  )

(defn- flatten-without-geometry [feature]
;; collection van parent is prefix, en voeg parents toe aan childs
  )

(defn- flatten [feature]
  (if (contains? feature :geometry)
    (flatten-with-geometry feature)
    (flatten-without-geometry feature)))

(defn- attributes [obj]
  (apply dissoc obj pdok-fields)
  )

(defn- collected-attributes [feature]
  "Returns a feature where the attributes are collected in :attributes"
  (let [collected (merge (:attributes feature) (attributes feature))
        no-attributes (select-keys feature pdok-fields)]
    (assoc no-attributes :attributes collected)))

;; TODO parent velden persisten
;; TODO change flatten moet nested closen bij nested met geometry
;; TODO alles wijzigen bij change bij nested zonder geometry
(defn process [processor feature]
  "Processes feature event. Returns nil or error reason"
  (let [flat-feature (flatten (collected-attributes feature))]
    (if (seq? flat-feature)
      (doseq [f flat-feature] (process processor f))
      (do; (println "FLAT: " flat-feature)
          (condp = (:action feature)
            :new (process-new-feature processor flat-feature)
            :change (process-change-feature processor flat-feature)
            (str "Cannot process: " flat-feature))))))

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
   {:persistence persistence
    :projectors projectors}))

(defn performance-test [count & args]
  (with-open [json (apply random-json-feature-stream "perftest" "col1" count args)]
    (let [processor (processor [(proj/geoserver-projector {:db-config proj/data-db})])
          features (features-from-stream json)]
      (time (do (doseq [f features] (process processor f))
                (shutdown processor)
                ))
      )))


; features-from-stream
