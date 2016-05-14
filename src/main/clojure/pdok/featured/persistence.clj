(ns pdok.featured.persistence
  (:refer-clojure :exclude [flush])
  (:require [pdok.cache :refer :all]
            [pdok.featured.dynamic-config :as dc]
            [pdok.postgres :as pg]
            [pdok.util :refer [with-bench]]
            [clojure.core.cache :as cache]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :as log]
            [clj-time [coerce :as tc]]
            [cognitect.transit :as transit]
            [clojure.core.async :as a
             :refer [>!! close! go chan]])
  (:import (clojure.lang PersistentQueue)))

(declare prepare-caches prepare-childs-cache prepare-parent-cache)

(defprotocol ProcessorPersistence
  (init [persistence for-dataset])
  (prepare [persistence features] "Called before processing this feature batch")
  (flush [persistence])
  (collections [persistence])
  (create-collection [persistence collection parent-collection])
  (child-collections [persistence parent-collection])
  (stream-exists? [persistence collection id])
  (create-stream
    [persistence collection id]
    [persistence collection id parent-collection parent-id parent-field])
  (append-to-stream [persistence version action collection id validity geometry attributes])
  (current-validity [persistence collection id])
  (last-action [persistence collection id])
  (current-version [persistence collection id])
  (childs
    [persistence parent-collection parent-id child-collection]
    [persistence parent-collection parent-id]
    "Returns seq of [collection id]")
  (parent [persistence collection id] "Returns [collection id field] tuple or nil")
  (get-last-n [persistence n collections]
    "Returns channel with n last stream entries for all collections in collections
If n nil => no limit, if collections nil => all collections")
  (close [persistence]))

(defn collection-tree [persistence root]
  "Returns root and all (child) collections in tree"
  (if-let [children (seq (child-collections persistence root))]
    (let [all-childs (mapcat #(collection-tree persistence %) children)]
      (cons root all-childs))
    [root]))

(defn path [persistence collection id]
  "Returns sequence of [collection id field child-id] tuples. Parents first. Root only means empty sequence"
  (when (and collection id)
    (if-let [[parent-collection parent-id parent-field] (parent persistence collection id)]
      (loop [path (list [parent-collection parent-id parent-field id])
             pc  parent-collection
             pid parent-id]
        (if-let [[new-pc new-pid new-pf] (parent persistence pc pid)]
          (recur (conj path [new-pc new-pid new-pf pid]) new-pc new-pid)
          path))
      (list))))

(defn root [persistence collection id]
  "Return [collection id] tuple with self if no other root"
  (let [path (path persistence collection id)]
    (if (empty? path)
      [collection id]
      (take 2 (first path)))))

(defn collection-exists?
  ([persistence collection]
    (let [collections (collections persistence)]
      (seq (filter #(= (:name %) collection) collections))))
  ([persistence collection parent-collection]
   ((collections persistence) {:name collection :parent-collection parent-collection})))

(defn schema [dataset]
  (str (name dc/*persistence-schema-prefix*) "_" dataset))

(defn- qualified-features [dataset]
  (str (pg/quoted (schema dataset)) "." (name dc/*persistence-features*)))

(defn- qualified-feature-stream [dataset]
  (str (pg/quoted (schema dataset)) "." (name dc/*persistence-feature-stream*)))

(defn- qualified-persistence-collections [dataset]
  (str (pg/quoted (schema dataset)) "." (name dc/*persistence-collections*)))

(defn- jdbc-init [db dataset]
  (when-not (pg/schema-exists? db (schema dataset))
    (pg/create-schema db (schema dataset)))
  (with-bindings {#'dc/*persistence-schema* (schema dataset)}
    (when-not (pg/table-exists? db dc/*persistence-schema* dc/*persistence-features*)
      (pg/create-table db dc/*persistence-schema* dc/*persistence-features*
                       [:id "bigserial" :primary :key]
                       [:collection "varchar(100)"]
                       [:feature_id "varchar(50)"]
                       [:parent_collection "varchar(255)"]
                       [:parent_id "varchar(50)"]
                       [:parent_field "varchar(255)"])
      (pg/create-index db dc/*persistence-schema* dc/*persistence-features* :collection :feature_id)
      (pg/create-index db dc/*persistence-schema* dc/*persistence-features* :parent_collection :parent_id))

    (when-not (pg/table-exists? db dc/*persistence-schema* dc/*persistence-feature-stream*)
      (pg/create-table db dc/*persistence-schema* dc/*persistence-feature-stream*
                       [:id "bigserial" :primary :key]
                       [:version "uuid"]
                       [:action "varchar(12)"]
                       [:collection "varchar(255)"]
                       [:feature_id "varchar(50)"]
                       [:validity "timestamp without time zone"]
                       [:geometry "text"]
                       [:attributes "text"])
      (pg/create-index db dc/*persistence-schema* dc/*persistence-feature-stream* :collection :feature_id))
    (when-not (pg/table-exists? db dc/*persistence-schema* dc/*persistence-collections*)
      (pg/create-table db dc/*persistence-schema* dc/*persistence-collections*
                       [:id "bigserial" :primary :key]
                       [:collection "varchar(255)"]
                       [:parent_collection "varchar(100)"])
      (pg/create-index db dc/*persistence-schema* dc/*persistence-collections*
                       :collection :parent_collection))))

(defn jdbc-create-collection [{:keys [db dataset]} collection parent-collection]
  (let [sql (str "INSERT INTO " (qualified-persistence-collections dataset)
                 " (collection, parent_collection) VALUES (?, ?)")]
    (j/execute! db (list sql collection parent-collection))))

(defn jdbc-all-collections [{:keys [db dataset]}]
  (j/with-db-connection [c db]
    (let [query (str "SELECT collection as name, parent_collection as \"parent-collection\" FROM "
                     (qualified-persistence-collections dataset))
          results (j/query c [query])]
      results)))

(defn jdbc-child-collections [{:keys [db dataset]} parent-collection]
  (j/with-db-connection [c db]
    (let [query (str "SELECT collection FROM " (qualified-persistence-collections dataset)
                     " WHERE parent_collection = ?")
          results (j/query c [query parent-collection])]
      (map :collection results))))

(defn record->feature [c record]
  (let [f (transient {})
        f (assoc! f
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

(defn jdbc-get-last-n [{:keys [db dataset]} n collections]
  (let [query (str "SELECT lastn.*,
  lag(version) OVER (PARTITION BY collection, feature_id ORDER BY id ASC) prev_version
  FROM (SELECT fs.id,
       fs.collection,
       fs.feature_id,
       fs.action,
       f.parent_collection,
       f.parent_id,
       fs.version,
       fs.validity,
       fs.attributes,
       fs.geometry
  FROM " (qualified-feature-stream dataset) " fs
  JOIN " (qualified-features dataset) " f ON fs.collection = f.collection AND
			     fs.feature_id = f.feature_id"
  (when (seq collections) (str " AND fs.collection in ("
                               (clojure.string/join "," (repeat (count collections) "?"))
                               ")"))
"  ORDER BY fs.id DESC"
  (when n (str " LIMIT " n))
  ") AS lastn ORDER BY id ASC")
        result-chan (chan)]
    (a/thread
      (j/with-db-connection [c db]
        (let [statement (j/prepare-statement
                         (doto (j/get-connection c) (.setAutoCommit false))
                         query :fetch-size 10000)]
          (j/query c (if-not collections [statement] (concat [statement] collections))
                   :row-fn (partial record->feature result-chan))
          (close! result-chan))))
    result-chan))

(defn- jdbc-create-stream
  ([{:keys [db dataset]} entries]
   (with-bench t (log/debug "Created streams in" t "ms")
     (try (j/with-db-connection [c db]
            (apply ( partial j/insert! c (qualified-features dataset) :transaction? (:transaction? db)
                             [:collection :feature_id :parent_collection :parent_id :parent_field])
                   entries))
          (catch java.sql.SQLException e
            (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e)))))))

(defn- jdbc-load-childs-cache [{:keys [db dataset]} parent-collection parent-ids]
  (try (j/with-db-connection [c db]
         (let [results
               (j/query c (apply vector (str "SELECT parent_id, collection, feature_id FROM " (qualified-features dataset)
                                " WHERE parent_collection = ? AND parent_id in ("
                                (clojure.string/join "," (repeat (count parent-ids) "?")) ")")
                                 parent-collection parent-ids))
               per-id (group-by :parent_id results)
               for-cache (map (fn [[parent-id values]]
                                [[parent-collection parent-id]
                                 (map (juxt :collection :feature_id) values)]) per-id)]
           for-cache))
       (catch java.sql.SQLException e
          (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e)))))

(defn- jdbc-load-parent-cache [{:keys [db dataset]} collection ids]
  (j/with-db-connection [c db]
    (let [results
          (j/query c (apply vector (str "SELECT feature_id, parent_collection, parent_id, parent_field FROM "
                           (qualified-features dataset)
                           " WHERE collection = ? AND feature_id in ("
                           (clojure.string/join "," (repeat (count ids) "?")) ")")
                            collection ids) :as-arrays? true)
          for-cache (map (fn [[feature-id parent-collection parent-id parent-field]]
                           [[collection feature-id] [parent-collection parent-id parent-field]])
                         (drop 1 results))]
      for-cache)))

(defn- jdbc-insert
  ([{:keys [db dataset]} entries]
   (with-bench t (log/debug "Inserted events in" t "ms")
     (try (j/with-db-connection [c db]
            (apply
             (partial j/insert! c (qualified-feature-stream dataset) :transaction? (:transaction? db)
                      [:version :action :collection :feature_id :validity :geometry :attributes])
             entries))
          (catch java.sql.SQLException e
            (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e)))))))

(defn- jdbc-load-cache [{:keys [db dataset]} collection ids]
  (when (seq ids)
    (try (let [results
               (j/with-db-connection [c db]
                 (j/query c (apply vector (str "SELECT DISTINCT ON (feature_id)
 collection, feature_id, validity, action, version FROM " (qualified-feature-stream dataset)
 " WHERE collection = ? and feature_id in (" (clojure.string/join "," (repeat (count ids) "?")) ")
   ORDER BY feature_id ASC, id DESC")
                                   collection ids) :as-arrays? true))]
           (map (fn [[collection id validity action version]] [[collection id]
                                                                      [validity (keyword action) version]] )
                (drop 1 results))
           )
         (catch java.sql.SQLException e
           (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e))))))

(defn filter-deleted [persistence childs]
  (filter (fn [[col id]] (not= :delete (last-action persistence col id)))
          childs))

(defn- append-cached-child [acc collection id _ _ _]
  (let [acc (if acc acc [])]
    (conj acc [collection id])))

(defn- current-state [persistence collection id]
  (let [key-fn (fn [collection id] [collection id])
        cached (use-cache (:stream-cache persistence) key-fn)]
      (cached collection id)))

(defn prepare-childs-cache [{:keys [db dataset] :as persistence} collection ids recur?]
  (when (and (not (nil? collection)) (seq ids))
    (when-let [not-nil-ids (seq (filter (complement nil?) ids))]
      (let [{:keys [db stream-cache childs-cache]} persistence
            for-childs-cache (jdbc-load-childs-cache persistence collection not-nil-ids)
            as-parents (group-by first (mapcat second for-childs-cache))]
        (apply-to-cache childs-cache for-childs-cache)
        (doseq [[collection grouped] as-parents]
          (let [new-ids (map second grouped)]
            (apply-to-cache stream-cache (jdbc-load-cache persistence collection new-ids))
            (when recur?
              (prepare-parent-cache persistence collection new-ids false)
              (prepare-childs-cache persistence collection new-ids true))))))))

(defn prepare-parent-cache [{:keys [db dataset] :as persistence} collection ids recur?]
  (when (and (not (nil? dataset)) (not (nil? collection)) (seq ids))
    (when-let [not-nil-ids (seq (filter (complement nil?) ids))]
      (let [{:keys [db stream-cache parent-cache]} persistence
            for-parents-cache (jdbc-load-parent-cache persistence collection not-nil-ids)
            as-childs (group-by first (map second for-parents-cache))]
        (apply-to-cache parent-cache for-parents-cache)
        (doseq [[collection grouped] as-childs]
          (let [new-ids (map second grouped)]
            (apply-to-cache stream-cache (jdbc-load-cache persistence collection new-ids))
            (when recur?
              (prepare-childs-cache persistence collection new-ids false)
              (prepare-parent-cache persistence collection new-ids true))))))))

(defn prepare-caches [persistence collection-id]
  (let [{:keys [db stream-cache childs-cache parent-cache]} persistence
        per-collection (group-by #(subvec % 0 1) collection-id)]
    (doseq [[[collection] grouped] per-collection]
      (let [ids (filter (complement nil?) (map #(nth % 1) grouped))]
        (apply-to-cache stream-cache (jdbc-load-cache persistence collection ids))
        (prepare-childs-cache persistence collection ids true)
        (prepare-parent-cache persistence collection ids true)))))

(defrecord CachedJdbcProcessorPersistence [db dataset collections-cache stream-batch stream-batch-size stream-cache
                                         link-batch link-batch-size childs-cache parent-cache]
  ProcessorPersistence
  (init [this for-dataset]
    (let [inited (assoc this :dataset for-dataset)
          _ (jdbc-init db for-dataset)
          _ (vreset! collections-cache (into #{} (jdbc-all-collections inited)))]
      inited))
  (prepare [this features]
    (with-bench t (log/debug "Prepared cache in" t "ms")
      (prepare-caches this (map (juxt :collection :id) features))
      (prepare-caches this (map (juxt :parent-collection :parent-id)
                                (filter #(and (:parent-collection %1) (:parent-id %1)) features))))
    this)
  (flush [this]
    (flush-batch stream-batch (partial jdbc-insert this))
    (flush-batch link-batch (partial jdbc-create-stream this))
    (dosync
     (vreset! stream-cache (cache/basic-cache-factory {}))
     (vreset! childs-cache (cache/basic-cache-factory {}))
     (vreset! parent-cache (cache/basic-cache-factory {})))
    this)
  (collections [_]
    @collections-cache)
  (create-collection [this collection parent-collection]
    (jdbc-create-collection this collection parent-collection)
    (vswap! collections-cache conj {:name collection :parent-collection parent-collection}))
  (child-collections [this parent-collection]
    (jdbc-child-collections this parent-collection))
  (stream-exists? [this collection id]
    (not (nil? (last-action this collection id))))
  (create-stream [this collection id]
    (create-stream this collection id nil nil nil))
  (create-stream [this collection id parent-collection parent-id parent-field]
    (let [childs-key-fn (fn [_ _ p-col p-id _] [p-col p-id])
          childs-value-fn append-cached-child
          parent-key-fn (fn [collection id _ _ _] [collection id])
          parent-value-fn (fn [_ _ _ pc pid pf] [pc pid pf])
          batched-create (batched link-batch link-batch-size :batch-fn (partial jdbc-create-stream this))
          cache-batched-create (with-cache childs-cache batched-create childs-key-fn childs-value-fn)
          double-cache-batched (with-cache parent-cache cache-batched-create parent-key-fn parent-value-fn)]
      (when-not (collection-exists? this collection parent-collection)
        (create-collection this collection parent-collection))
      (double-cache-batched collection id parent-collection parent-id parent-field)))
  (append-to-stream [this version action collection id validity geometry attributes]
    ;(println "APPEND: " id)
    (let [key-fn   (fn [_ _ collection id _ _ _] [collection id])
          value-fn (fn [_ version action _ _ validity _ _] [validity action version])
          batched-insert (batched stream-batch stream-batch-size :batch-fn (partial jdbc-insert this))
          cache-batched-insert (with-cache stream-cache batched-insert key-fn value-fn)]
      (cache-batched-insert version action collection id validity geometry attributes)))
  (current-validity [this collection id]
    (get (current-state this collection id) 0))
  (last-action [this collection id]
    (get (current-state this collection id) 1))
  (current-version [this collection id]
    (get (current-state this collection id) 2))
  (childs [this parent-collection parent-id child-collection]
    (let [key-fn (fn [parent-collection parent-id _]
                   [parent-collection parent-id])
          cached (use-cache childs-cache key-fn)]
      (filter-deleted this
                      (->> (cached parent-collection parent-id child-collection)
                           (filter #(= (first %)))))))
  (childs [this parent-collection parent-id]
    (let [key-fn (fn [parent-collection parent-id]
                   [parent-collection parent-id])
          cached (use-cache childs-cache key-fn)]
      (filter-deleted this (cached parent-collection parent-id))))
  (parent [_ collection id]
    (let [key-fn (fn [collection id] [collection id])
          cached (use-cache parent-cache key-fn)
          q-result (cached collection id)]
      (if (some #(= nil %) q-result)
        nil
        q-result)))
  (get-last-n [this n collections]
    (jdbc-get-last-n this n collections))
  (close [this]
    (flush this)))

(defn make-cached-jdbc-processor-persistence [config]
  (let [db (:db-config config)
        collections-cache (volatile! #{})
        stream-batch-size (or (:stream-batch-size config) (:batch-size config) 10000)
        stream-batch (volatile! (PersistentQueue/EMPTY))
        stream-cache (volatile! (cache/basic-cache-factory {}))
        link-batch-size (or (:link-batch-size config) (:batch-size config) 10000)
        link-batch (volatile! (PersistentQueue/EMPTY))
        childs-cache (volatile! (cache/basic-cache-factory {}))
        parent-cache (volatile! (cache/basic-cache-factory {}))]
    (CachedJdbcProcessorPersistence. db "unknown-dataset" collections-cache stream-batch stream-batch-size stream-cache
                                     link-batch link-batch-size childs-cache parent-cache)))

(defrecord NoStatePersistence []
    ProcessorPersistence
  (init [this for-dataset]
    this)
  (prepare [this _]
    this)
  (flush [this]
    this)
  (collections [persistence]
    #{})
  (create-collection [persistence collection parent-collection]
    nil)
  (child-collections [persistence parent-collection]
    nil)
  (stream-exists? [persistence collection id]
    false)
  (create-stream [persistence collection id]
    nil)
  (create-stream [persistence collection id parent-collection parent-id parent-field]
    nil)
  (append-to-stream [persistence version action collection id validity geometry attributes]
    nil)
  (current-validity [persistence collection id]
    nil)
  (last-action [persistence collection id]
    nil)
  (current-version [persistence collection id]
    nil)
  (childs [persistence parent-collection parent-id child-collection]
    nil)
  (childs [persistence parent-collection parent-id]
    nil)
  (parent [persistence collection id]
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
