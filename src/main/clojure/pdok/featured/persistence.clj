(ns pdok.featured.persistence
  (:require [pdok.cache :refer :all]
            [pdok.postgres :as pg]
            [clojure.core.cache :as cache]
            [clojure.java.jdbc :as j]
            [clj-time [coerce :as tc]]
            [cognitect.transit :as transit])
  (:import [java.io ByteArrayOutputStream]))

(defprotocol ProcessorPersistence
  (init [this])
  (stream-exists? [this dataset collection id])
  (create-stream
    [this dataset collection id]
    [this dataset collection id parent-collection parent-id])
  (append-to-stream [this version action dataset collection id validity geometry attributes])
  (current-validity [this dataset collection id])
  (last-action [this dataset collection id])
  (childs [this dataset parent-collection parent-id child-collection])
  (parent [this dataset collection id] "Returns [collection id] tuple or nil")
  (close [_])
  )

(defn path [persistence dataset collection id]
  "Returns sequence of [collection id] tuples. Parents first. Root only means empty sequence"
  (when (and dataset collection id)
    (if-let [[parent-collection parent-id] (parent persistence dataset collection id)]
      (loop [path (list)
             pc  parent-collection
             pid parent-id]
        (if-let [[new-pc new-pid] (parent persistence dataset pc pid)]
          (recur (conj path [new-pc new-pid]) new-pc new-pid)
          path))
      (list))))

(defn root [persistence dataset collection id]
  "Return [collection id] tuple with self if no other root"
  (let [path (path persistence dataset collection id)]
    (if (empty? path)
      [collection id]
      (first path))))

(def ^:dynamic *jdbc-schema* :featured)
(def ^:dynamic *jdbc-features* :feature)
(def ^:dynamic *jdbc-feature-stream* :feature_stream)

(defn- qualified-features []
  (str  (name *jdbc-schema*) "." (name *jdbc-features*)))

(defn- qualified-feature-stream []
  (str  (name *jdbc-schema*) "." (name *jdbc-feature-stream*)))

(defn- jdbc-init [db]
  (when-not (pg/schema-exists? db *jdbc-schema*)
    (pg/create-schema db *jdbc-schema*))
  (when-not (pg/table-exists? db *jdbc-schema* *jdbc-features*)
    (pg/create-table db *jdbc-schema* *jdbc-features*
                     [:id "bigserial" :primary :key]
                     [:dataset "varchar(100)"]
                     [:collection "varchar(100)"]
                     [:feature_id "varchar(50)"]
                     [:parent_collection "varchar(255)"]
                     [:parent_id "varchar(50)"]
                     [:closed "boolean" "default false"])
    (pg/create-index db *jdbc-schema* *jdbc-features* :dataset :collection :feature_id)
    (pg/create-index db *jdbc-schema* *jdbc-features* :dataset :parent_collection :parent_id))
  (when-not (pg/table-exists? db *jdbc-schema* *jdbc-feature-stream*)
    (pg/create-table db *jdbc-schema* *jdbc-feature-stream*
                     [:id "bigserial" :primary :key]
                     [:version "uuid"]
                     [:action "character(12)"]
                     [:dataset "varchar(100)"]
                     [:collection "varchar(255)"]
                     [:feature_id "varchar(50)"]
                     [:validity "timestamp without time zone"]
                     [:geometry "text"]
                     [:attributes "text"])
    (pg/create-index db *jdbc-schema* *jdbc-feature-stream* :dataset :collection :feature_id)))

(defn- jdbc-create-stream
  ([db dataset collection id parent-collection parent-id]
   (jdbc-create-stream db (list [dataset collection id parent-collection parent-id])))
  ([db entries]
   (j/with-db-connection [c db]
     (apply ( partial j/insert! c (qualified-features) :transaction? false?
                      [:dataset :collection :feature_id :parent_collection :parent_id])
            entries))))

(defn- jdbc-close-stream
  ([db dataset collection id]
   (jdbc-create-stream db (list [dataset collection id])))
  ([db entries]
   (let [sql (str "UPDATE " (qualified-features) " SET closed = true
WHERE dataset = ? AND collection = ?  AND feature_id = ?")]
     (j/execute! db (cons sql entries) :multi? true :transaction? false))))

(defn- jdbc-load-childs-cache [db dataset parent-collection child-collection]
  (j/with-db-connection [c db]
    (let [results
          (j/query c [(str "SELECT parent_id, feature_id FROM " (qualified-features)
                           " WHERE dataset = ?  AND parent_collection = ? AND collection = ? AND closed = false ")
                      dataset parent-collection child-collection])
          per-id (group-by :parent_id results)
          for-cache (map (fn [[parent-id values]]
                           [[dataset parent-collection parent-id child-collection]
                            (map :feature_id values)]) per-id)]
      for-cache)))

(defn- jdbc-get-childs [db dataset parent-collection parent-id child-collection]
  (j/with-db-connection [c db]
    (let [results
          (j/query c [(str "SELECT feature_id FROM " (qualified-features)
" WHERE dataset = ? AND parent_collection = ?  AND parent_id = ? AND collection = ?  AND closed = false")
                      dataset parent-collection parent-id child-collection] :as-arrays? true)]
      results)))

(defn- jdbc-load-parent-cache [db dataset collection]
  (j/with-db-connection [c db]
    (let [results
          (j/query c [(str "SELECT feature_id, parent_collection, parent_id FROM " (qualified-features)
                           " WHERE dataset = ? AND collection = ?")
                      dataset collection] :as-arrays? true)
          for-cache (map (fn [[feature-id parent-collection parent-id]]
                           [dataset collection feature-id] [parent-collection parent-id])
                         (drop 1 results))]
      for-cache)))

(defn- jdbc-get-parent [db dataset collection id]
  (j/with-db-connection [c db]
    (let [result
          (j/query c [(str "SELECT parent_collection, parent_id FROM " (qualified-features)
                           " WHERE dataset = ? AND collection = ? AND feature_id = ?"
                           " AND parent_collection is not null")
                      dataset collection id] :as-arrays? true)]
      (first result))))

(defn- jdbc-insert
  ([db version action dataset collection id validity geometry attributes]
   (jdbc-insert db (list [version action dataset collection id validity geometry attributes])))
  ([db entries]
   (try (j/with-db-connection [c db]
          (apply
           (partial j/insert! c (qualified-feature-stream) :transaction? false
                    [:version :action :dataset :collection :feature_id :validity :geometry :attributes])
           entries)
            )
        (catch java.sql.SQLException e (j/print-sql-exception-chain e)))))

(defn- jdbc-load-cache [db dataset collection]
  (let [results
        (j/with-db-connection [c db]
          (j/query c [(str "SELECT dataset, collection, feature_id, validity as cur_val, action as last_action FROM (
 SELECT dataset, collection, feature_id, action, validity,
 row_number() OVER (PARTITION BY dataset, collection, feature_id ORDER BY id DESC) AS rn
 FROM " (qualified-feature-stream)
" WHERE dataset = ? AND collection = ?) a
WHERE rn = 1")
                      dataset collection] :as-arrays? true))]
    (map (fn [f] (split-at 3 f)) results)
    )
  )

(defn- jdbc-last-stream-validity-and-action [db dataset collection id]
   (j/with-db-connection [c db]
     (let [results
           (j/query c ["SELECT action, validity FROM (
 SELECT action, validity,
 row_number() OVER (PARTITION BY dataset, collection, feature_id ORDER BY id DESC) AS rn
 FROM " (qualified-feature-stream)
" WHERE dataset = ? AND collection = ? AND feature_id = ?) a
WHERE rn = 1"
                       dataset collection id] :as-arrays? true)]
       (-> results first))))

(deftype JdbcProcessorPersistence [db]
  ProcessorPersistence
  (init [this] (jdbc-init db) this)
  (stream-exists? [_ dataset collection id]
    (let [current-validity (partial jdbc-last-stream-validity-and-action db)]
      (not (nil? (current-validity dataset collection id)))))
  (create-stream [this dataset collection id]
    (create-stream this dataset collection id nil nil))
  (create-stream [_ dataset collection id parent-collection parent-id]
    (jdbc-create-stream db dataset collection id parent-collection parent-id))
  (append-to-stream [_ version action dataset collection id validity geometry attributes]
    ; compose with nil, because insert returns record. Should fix this...
    (jdbc-insert db version action dataset collection id validity geometry attributes)
    nil)
  (current-validity [_ dataset collection id]
    (-> (jdbc-last-stream-validity-and-action db dataset collection id) first))
  (last-action [_ dataset collection id]
    (-> (jdbc-last-stream-validity-and-action db dataset collection id) second))
  (childs [_ dataset parent-collection parent-id child-collection]
    (jdbc-get-childs db dataset parent-collection parent-id child-collection))
  (parent [_ dataset collection id]
    (jdbc-get-parent db dataset collection id))
  (close [this] this)
  )

(defn- append-cached-child [acc _ _ id _ _]
  (let [acc (if acc acc [])]
    (conj acc id)))

(deftype CachedJdbcProcessorPersistence [db stream-batch stream-batch-size stream-cache stream-load-cache?
                                         link-batch link-batch-size childs-cache childs-load-cache?
                                         parent-cache parent-load-cache?]
  ProcessorPersistence
  (init [this] (jdbc-init db) this)
  (stream-exists? [this dataset collection id]
    (not (nil? (current-validity this dataset collection id))))
  (create-stream [this dataset collection id]
    (create-stream this dataset collection id nil nil))
  (create-stream [_ dataset collection id parent-collection parent-id]
    (let [childs-key-fn (fn [dataset collection _ p-col p-id] [dataset p-col p-id collection])
          childs-value-fn append-cached-child
          parent-key-fn (fn [dataset collection id _ _] [dataset collection id])
          parent-value-fn (fn [_ _ _ _ pc pid] [pc pid])
          batched (with-batch link-batch link-batch-size (partial jdbc-create-stream db))
          cache-batched (with-cache childs-cache batched childs-key-fn childs-value-fn)
          double-cache-batched (with-cache parent-cache cache-batched parent-key-fn parent-value-fn)]
      (double-cache-batched dataset collection id parent-collection parent-id)))
  (append-to-stream [_ version action dataset collection id validity geometry attributes]
    (let [key-fn   (fn [_ _ dataset collection id _ _ _] [dataset collection id])
          value-fn (fn [_ _ action _ _ _ validity _ _] [validity action])
          batched (with-batch stream-batch stream-batch-size (partial jdbc-insert db))
          cache-batched (with-cache stream-cache batched key-fn value-fn)]
      (cache-batched version action dataset collection id validity geometry attributes)))
  (current-validity [_ dataset collection id]
    (let [key-fn (fn [dataset collection id] [dataset collection id])
          load-cache (fn [dataset collection id]
                       (when (stream-load-cache? dataset collection) (jdbc-load-cache db dataset collection)))
          cached (use-cache stream-cache key-fn load-cache)]
      (first (cached dataset collection id))))
  (last-action [_ dataset collection id]
    (let [key-fn (fn [dataset collectioni id] [dataset collection id])
          load-cache (fn [dataset collection id]
                       (when (stream-load-cache? dataset collection) (jdbc-load-cache db dataset collection)))
          cached (use-cache stream-cache key-fn load-cache)]
      (second (cached dataset collection id))))
  (childs [_ dataset parent-collection parent-id child-collection]
    (let [key-fn (fn [dataset parent-collection parent-id child-collection]
                   [dataset parent-collection parent-id child-collection])
          load-cache (fn [dataset parent-collection _ collection]
                       (when (childs-load-cache? dataset parent-collection child-collection)
                         (jdbc-load-childs-cache db dataset parent-collection child-collection)))
          cached (use-cache childs-cache key-fn load-cache)]
      (cached dataset parent-collection parent-id child-collection)))
  (parent [_ dataset collection id]
    (let [key-fn (fn [dataset collection id] [dataset collection id])
          load-cache (fn [dataset collection _]
                       (when (parent-load-cache? dataset collection)
                         (jdbc-load-parent-cache db dataset collection)))
          cached (use-cache parent-cache key-fn load-cache)
          q-result (cached dataset collection id)]
      (if (some #(= nil %) q-result)
        nil
        q-result)))
  (close [this]
    (flush-batch stream-batch (partial jdbc-insert db))
    (flush-batch link-batch (partial jdbc-create-stream db))
    this)
  )

(defn jdbc-processor-persistence [config]
  (let [db (:db-config config)]
    (JdbcProcessorPersistence. db)))

(defn cached-jdbc-processor-persistence [config]
  (let [db (:db-config config)
        stream-batch-size (or (:stream-batch-size config) (:batch-size config) 10000)
        stream-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        stream-cache (ref (cache/basic-cache-factory {}))
        stream-load-cache? (once-true-fn)
        link-batch-size (or (:link-batch-size config) (:batch-size config) 10000)
        link-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        childs-cache (ref (cache/basic-cache-factory {}))
        childs-load-cache? (once-true-fn)
        parent-cache (ref (cache/basic-cache-factory {}))
        parent-load-cache? (once-true-fn)]
    (CachedJdbcProcessorPersistence. db stream-batch stream-batch-size stream-cache stream-load-cache?
                                     link-batch link-batch-size childs-cache childs-load-cache?
                                     parent-cache parent-load-cache?)))

;; (def ^:private pgdb {:subprotocol "postgresql"
;;                      :subname "//localhost:5432/pdok"
;;                      :user "postgres"
;;                      :password "postgres"})

;(def pers ( processor-jdbc-persistence {:db-config pgdb}))
;((:append-to-stream pers) "set" "col" "1" (tl/local-now))
;((:shutdown pers))
