(ns pdok.featured.persistence
  (:refer-clojure :exclude [flush])
  (:require [pdok.cache :refer :all]
            [pdok.featured.dynamic-config :as dc]
            [pdok.postgres :as pg]
            [pdok.util :refer [with-bench]]
            [joplin.core :as joplin]
            [joplin.jdbc.database]
            [clojure.core.cache :as cache]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :as log]
            [clj-time [coerce :as tc]]
            [cognitect.transit :as transit]
            [clojure.core.async :as a
             :refer [>!! close! go chan]])
  (:import [java.io ByteArrayOutputStream]))

(declare prepare-caches prepare-childs-cache prepare-parent-cache)

(defprotocol ProcessorPersistence
  (init [this])
  (prepare [this features] "Called before processing this feature batch")
  (flush [persistence])
  (child-collections [this dataset parent-collection])
  (stream-exists? [persistence dataset collection id])
  (create-stream
    [persistence dataset collection id]
    [persistence dataset collection id parent-collection parent-id parent-field])
  (append-to-stream [persistence version action dataset collection id validity geometry attributes])
  (current-validity [persistence dataset collection id])
  (last-action [persistence dataset collection id])
  (current-version [persistence dataset collection id])
  (childs
    [persistence dataset parent-collection parent-id child-collection]
    [persistence dataset parent-collection parent-id]
    "Returns seq of [collection id]")
  (parent [persistence dataset collection id] "Returns [collection id field] tuple or nil")
  (get-last-n [persistence dataset n collections]
    "Returns channel with n last stream entries for all collections in collections
If n nil => no limit, if collections nil => all collections")
  (close [persistence]))

(defn collection-tree [persistence dataset root]
  "Returns root and all (child) collections in tree"
  (if-let [children (seq (child-collections persistence dataset root))]
    (let [all-childs (mapcat #(collection-tree persistence dataset %) children)]
      (cons root all-childs))
    [root]))

(defn path [persistence dataset collection id]
  "Returns sequence of [collection id field child-id] tuples. Parents first. Root only means empty sequence"
  (when (and dataset collection id)
    (if-let [[parent-collection parent-id parent-field] (parent persistence dataset collection id)]
      (loop [path (list [parent-collection parent-id parent-field id])
             pc  parent-collection
             pid parent-id]
        (if-let [[new-pc new-pid new-pf] (parent persistence dataset pc pid)]
          (recur (conj path [new-pc new-pid new-pf pid]) new-pc new-pid)
          path))
      (list))))

(defn root [persistence dataset collection id]
  "Return [collection id] tuple with self if no other root"
  (let [path (path persistence dataset collection id)]
    (if (empty? path)
      [collection id]
      (take 2 (first path)))))

(defn- qualified-features []
  (str  (name dc/*persistence-schema*) "." (name dc/*persistence-features*)))

(defn- qualified-feature-stream []
  (str  (name dc/*persistence-schema*) "." (name dc/*persistence-feature-stream*)))

(defn- qualified-persistence-migrations []
  (str (name dc/*persistence-schema*) "." (name dc/*persistence-migrations*)))

(defn- qualified-persistence-collections []
  (str (name dc/*persistence-schema*) "." (name dc/*persistence-collections*)))

(defn- jdbc-init [db]
  (when-not (pg/schema-exists? db dc/*persistence-schema*)
    (pg/create-schema db dc/*persistence-schema*))
  (let [jdb {:db (assoc db
                        :type :jdbc
                        :url (pg/dbspec->url db))
             :migrator "/pdok/featured/migrations/persistence"
             :migrations-table (qualified-persistence-migrations)}]
    (log/with-logs ['pdok.featured.persistence :trace :error]
      (joplin/migrate-db jdb))))

(defn collection-exists? [db dataset collection parent-collection]
  (j/with-db-connection [c db]
    (let [results (j/query c [(str "SELECT id FROM " (qualified-persistence-collections)
                                   " WHERE "
                                   (pg/map->where-clause {:dataset dataset
                                                          :collection collection
                                                          :parent_collection parent-collection}))])]
      (not (nil? (first results))))))

(defn create-collection [db dataset collection parent-collection]
  (let [sql (str "INSERT INTO " (qualified-persistence-collections)
                 " (dataset, collection, parent_collection) VALUES (?, ?, ?)")]
    (j/execute! db (list sql dataset collection parent-collection))))

(defn jdbc-child-collections [db dataset parent-collection]
  (j/with-db-connection [c db]
    (let [query (str "SELECT collection FROM " (qualified-persistence-collections)
                     " WHERE dataset = ? AND parent_collection = ?")
          results (j/query c [query dataset parent-collection])]
      (map :collection results))))

(defn record->feature [c record]
  (let [f (transient {})
        f (assoc! f
                 :dataset (:dataset record)
                 :collection (:collection record)
                 :id (:feature_id record)
                 :action (keyword (:action record))
                 :version (:version record)
                 :current-version (:prev_version record)
                 :validity (:validity record)
                 :geometry (pg/from-json (:geometry record))
                 :attributes (pg/from-json (:attributes record)))
        f (cond-> f
            (:parent_collection record) (assoc! :parent-collection (:parent_collection record))
            (:parent_id record) (assoc! :parent-id (:parent_id record)))]
    (>!! c (persistent! f))))

(defn jdbc-get-last-n [persistence dataset n collections]
  (let [query (str "SELECT lastn.*,
  lag(version) OVER (PARTITION BY dataset, collection, feature_id ORDER BY id ASC) prev_version
  FROM (SELECT fs.id,
       fs.dataset,
       fs.collection,
       fs.feature_id,
       fs.action,
       f.parent_collection,
       f.parent_id,
       fs.version,
       fs.validity,
       fs.attributes,
       fs.geometry
  FROM " (qualified-feature-stream) " fs
  JOIN " (qualified-features) " f ON fs.dataset = f.dataset AND
			     fs.collection = f.collection AND
			     fs.feature_id = f.feature_id
  WHERE fs.dataset = ?"
  (when (seq collections) (str " AND fs.collection in ("
                               (clojure.string/join "," (repeat (count collections) "?"))
                               ")"))
"  ORDER BY fs.id DESC"
  (when n (str " LIMIT " n))
  ") AS lastn ORDER BY id ASC")
        result-chan (chan)]
    (a/thread
      (j/with-db-connection [c (:db persistence)]
        (let [statement (j/prepare-statement
                         (doto (j/get-connection c) (.setAutoCommit false))
                         query :fetch-size 10000)]
          (j/query c (if-not collections [statement dataset] (concat [statement dataset] collections))
                   :row-fn (partial record->feature result-chan))
          (close! result-chan))))
    result-chan))

(defn- jdbc-create-stream
  ([db entries]
   (with-bench t (log/debug "Created streams in" t "ms")
     (try (j/with-db-connection [c db]
            (apply ( partial j/insert! c (qualified-features) :transaction? false?
                             [:dataset :collection :feature_id :parent_collection :parent_id :parent_field])
                   entries))
          (catch java.sql.SQLException e
            (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e)))))))

(defn- jdbc-load-childs-cache [db dataset parent-collection parent-ids]
  (try (j/with-db-connection [c db]
         (let [results
               (j/query c (apply vector (str "SELECT parent_id, collection, feature_id FROM " (qualified-features)
                                " WHERE dataset = ?  AND parent_collection = ? AND parent_id in ("
                                (clojure.string/join "," (repeat (count parent-ids) "?")) ")")
                           dataset parent-collection parent-ids))
               per-id (group-by :parent_id results)
               for-cache (map (fn [[parent-id values]]
                                [[dataset parent-collection parent-id]
                                 (map (juxt :collection :feature_id) values)]) per-id)]
           for-cache))
       (catch java.sql.SQLException e
          (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e)))))

(defn- jdbc-load-parent-cache [db dataset collection ids]
  (j/with-db-connection [c db]
    (let [results
          (j/query c (apply vector (str "SELECT feature_id, parent_collection, parent_id, parent_field FROM "
                           (qualified-features)
                           " WHERE dataset = ? AND collection = ? AND feature_id in ("
                           (clojure.string/join "," (repeat (count ids) "?")) ")")
                      dataset collection ids) :as-arrays? true)
          for-cache (map (fn [[feature-id parent-collection parent-id parent-field]]
                           [[dataset collection feature-id] [parent-collection parent-id parent-field]])
                         (drop 1 results))]
      for-cache)))

(defn- jdbc-insert
  ([db entries]
   (with-bench t (log/debug "Inserted events in" t "ms")
     (try (j/with-db-connection [c db]
            (apply
             (partial j/insert! c (qualified-feature-stream) :transaction? false
                      [:version :action :dataset :collection :feature_id :validity :geometry :attributes])
             entries)
            )
          (catch java.sql.SQLException e
            (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e)))))))

(defn- jdbc-load-cache [db dataset collection ids]
  (when (seq ids)
    (try (let [results
               (j/with-db-connection [c db]
                 (j/query c (apply vector (str "SELECT dataset, collection, feature_id, validity, action, version FROM (
 SELECT dataset, collection, feature_id, action, validity, version,
 row_number() OVER (PARTITION BY dataset, collection, feature_id ORDER BY id DESC) AS rn
 FROM " (qualified-feature-stream)
 " WHERE dataset = ? AND collection = ? and feature_id in ("
 (clojure.string/join "," (repeat (count ids) "?"))
 ")) a WHERE rn = 1")
                                   dataset collection ids) :as-arrays? true))]
           (map (fn [[dataset collection id validity action version]] [[dataset collection id]
                                                                      [validity (keyword action) version]] )
                (drop 1 results))
           )
         (catch java.sql.SQLException e
           (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e))))))

(defn filter-deleted [persistence dataset childs]
  (filter (fn [[col id]] (not= :delete (last-action persistence dataset col id)))
          childs))

(defn- append-cached-child [acc _ collection id _ _ _]
  (let [acc (if acc acc [])]
    (conj acc [collection id])))

(defn- current-state [persistence dataset collection id]
  (let [key-fn (fn [dataset collectioni id] [dataset collection id])
        cached (use-cache (:stream-cache persistence) key-fn)]
      (cached dataset collection id)))

(defn prepare-childs-cache [persistence dataset collection ids recur?]
  (when (and (not (nil? dataset)) (not (nil? collection)) (seq ids))
    (when-let [not-nil-ids (seq (filter (complement nil?) ids))]
      (let [{:keys [db stream-cache childs-cache]} persistence
            for-childs-cache (jdbc-load-childs-cache db dataset collection not-nil-ids)
            as-parents (group-by first (mapcat second for-childs-cache))]
        (apply-to-cache childs-cache for-childs-cache)
        (doseq [[collection grouped] as-parents]
          (let [new-ids (map second grouped)]
            (apply-to-cache stream-cache (jdbc-load-cache db dataset collection new-ids))
            (when recur?
              (prepare-parent-cache persistence dataset collection new-ids false)
              (prepare-childs-cache persistence dataset collection new-ids true))))))))

(defn prepare-parent-cache [persistence dataset collection ids recur?]
  (when (and (not (nil? dataset)) (not (nil? collection)) (seq ids))
    (when-let [not-nil-ids (seq (filter (complement nil?) ids))]
      (let [{:keys [db stream-cache parent-cache]} persistence
            for-parents-cache (jdbc-load-parent-cache db dataset collection not-nil-ids)
            as-childs (group-by first (map second for-parents-cache))]
        (apply-to-cache parent-cache for-parents-cache)
        (doseq [[collection grouped] as-childs]
          (let [new-ids (map second grouped)]
            (apply-to-cache stream-cache (jdbc-load-cache db dataset collection new-ids))
            (when recur?
              (prepare-childs-cache persistence dataset collection new-ids false)
              (prepare-parent-cache persistence dataset collection new-ids true))))))))

(defn prepare-caches [persistence dataset-collection-id]
  (let [{:keys [db stream-cache childs-cache parent-cache]} persistence
        dataset-collection (group-by #(subvec % 0 2) dataset-collection-id)]
    (doseq [[[dataset collection] grouped] dataset-collection]
      (let [ids (filter (complement nil?) (map #(nth % 2) grouped))]
        (apply-to-cache stream-cache (jdbc-load-cache db dataset collection ids))
        (prepare-childs-cache persistence dataset collection ids true)
        (prepare-parent-cache persistence dataset collection ids true)))))

(defrecord CachedJdbcProcessorPersistence [db collection-cache stream-batch stream-batch-size stream-cache
                                         link-batch link-batch-size childs-cache parent-cache]
  ProcessorPersistence
  (init [this] (jdbc-init db) this)
  (prepare [this features]
    (with-bench t (log/debug "Prepared cache in" t "ms")
      (prepare-caches this (map (juxt :dataset :collection :id) features)))
    this)
  (flush [this]
    (flush-batch stream-batch (partial jdbc-insert db))
    (flush-batch link-batch (partial jdbc-create-stream db))
    (dosync
     (ref-set stream-cache (cache/basic-cache-factory {}))
     (ref-set childs-cache (cache/basic-cache-factory {}))
     (ref-set parent-cache (cache/basic-cache-factory {})))
    this)
  (child-collections [this dataset parent-collection]
    (jdbc-child-collections db dataset parent-collection))
  (stream-exists? [this dataset collection id]
    (not (nil? (last-action this dataset collection id))))
  (create-stream [this dataset collection id]
    (create-stream this dataset collection id nil nil nil))
  (create-stream [_ dataset collection id parent-collection parent-id parent-field]
    (let [cached-collection-exists? (cached collection-cache collection-exists? db)
          childs-key-fn (fn [dataset _ _ p-col p-id _] [dataset p-col p-id])
          childs-value-fn append-cached-child
          parent-key-fn (fn [dataset collection id _ _ _] [dataset collection id])
          parent-value-fn (fn [_ _ _ _ pc pid pf] [pc pid pf])
          batched-create (batched link-batch link-batch-size :batch-fn (partial jdbc-create-stream db))
          cache-batched-create (with-cache childs-cache batched-create childs-key-fn childs-value-fn)
          double-cache-batched (with-cache parent-cache cache-batched-create parent-key-fn parent-value-fn)]
      (when-not (cached-collection-exists? dataset collection parent-collection)
        (create-collection db dataset collection parent-collection)
        (cached-collection-exists? :reload dataset collection parent-collection))
      (double-cache-batched dataset collection id parent-collection parent-id parent-field)))
  (append-to-stream [_ version action dataset collection id validity geometry attributes]
    ;(println "APPEND: " id)
    (let [key-fn   (fn [_ _ dataset collection id _ _ _] [dataset collection id])
          value-fn (fn [_ version action _ _ _ validity _ _] [validity action version])
          batched-insert (batched stream-batch stream-batch-size :batch-fn (partial jdbc-insert db))
          cache-batched-insert (with-cache stream-cache batched-insert key-fn value-fn)]
      (cache-batched-insert version action dataset collection id validity geometry attributes)))
  (current-validity [this dataset collection id]
    (get (current-state this dataset collection id) 0))
  (last-action [this dataset collection id]
    (get (current-state this dataset collection id) 1))
  (current-version [this dataset collection id]
    (get (current-state this dataset collection id) 2))
  (childs [this dataset parent-collection parent-id child-collection]
    (let [key-fn (fn [dataset parent-collection parent-id _]
                   [dataset parent-collection parent-id])
          cached (use-cache childs-cache key-fn)]
      (filter-deleted this dataset
                      (->> (cached dataset parent-collection parent-id child-collection)
                           (filter #(= (first %)))))))
  (childs [this dataset parent-collection parent-id]
    (let [key-fn (fn [dataset parent-collection parent-id]
                   [dataset parent-collection parent-id])
          cached (use-cache childs-cache key-fn)]
      (filter-deleted this dataset (cached dataset parent-collection parent-id))))
  (parent [_ dataset collection id]
    (let [key-fn (fn [dataset collection id] [dataset collection id])
          cached (use-cache parent-cache key-fn)
          q-result (cached dataset collection id)]
      (if (some #(= nil %) q-result)
        nil
        q-result)))
  (get-last-n [this dataset n collections]
    (jdbc-get-last-n this dataset n collections))
  (close [this]
    (flush this)))

(defn cached-jdbc-processor-persistence [config]
  (let [db (:db-config config)
        collection-cache (atom {})
        stream-batch-size (or (:stream-batch-size config) (:batch-size config) 10000)
        stream-batch (volatile! (clojure.lang.PersistentQueue/EMPTY))
        stream-cache (ref (cache/basic-cache-factory {}))
        link-batch-size (or (:link-batch-size config) (:batch-size config) 10000)
        link-batch (volatile! (clojure.lang.PersistentQueue/EMPTY))
        childs-cache (ref (cache/basic-cache-factory {}))
        parent-cache (ref (cache/basic-cache-factory {}))]
    (CachedJdbcProcessorPersistence. db collection-cache stream-batch stream-batch-size stream-cache
                                     link-batch link-batch-size childs-cache parent-cache)))

(defrecord NoStatePersistence []
    ProcessorPersistence
  (init [this]
    this)
  (prepare [this _]
    this)
  (flush [this]
    this)
  (child-collections [this dataset parent-collection]
    nil)
  (stream-exists? [persistence dataset collection id]
    false)
  (create-stream [persistence dataset collection id]
    nil)
  (create-stream [persistence dataset collection id parent-collection parent-id parent-field]
    nil)
  (append-to-stream [persistence version action dataset collection id validity geometry attributes]
    nil)
  (current-validity [persistence dataset collection id]
    nil)
  (last-action [persistence dataset collection id]
    nil)
  (current-version [persistence dataset collection id]
    nil)
  (childs [persistence dataset parent-collection parent-id child-collection]
    nil)
  (childs [persistence dataset parent-collection parent-id]
    nil)
  (parent [persistence dataset collection id]
    nil)
  (close [this]
    this))


(defn make-no-state []
  (->NoStatePersistence))
;; (def ^:private pgdb {:subprotocol "postgresql"
;;                      :subname "//localhost:5432/pdok"
;;                      :user "postgres"
;;                      :password "postgres"})

;(def pers ( processor-jdbc-persistence {:db-config pgdb}))
;((:append-to-stream pers) "set" "col" "1" (tl/local-now))
;((:shutdown pers))
