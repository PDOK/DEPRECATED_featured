(ns pdok.featured.persistence
  (:refer-clojure :exclude [flush])
  (:require [pdok.cache :refer :all]
            [pdok.featured.dynamic-config :as dc]
            [pdok.postgres :as pg]
            [pdok.util :refer [with-bench checked partitioned]]
            [pdok.transit :as transit]
            [clojure.core.cache :as cache]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a
             :refer [>!! close! go chan]])
  (:import (clojure.lang PersistentQueue)
           (java.sql SQLException)))

(declare prepare-caches prepare-childs-cache prepare-parent-cache)

(def ^Integer jdbc-collection-partition-size 30000)

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
  (get-range [persistence start end collections]
    "Returns channel with entries in range from start (inclusive) to end (inclusive)
     for all collections in collections. Start and end are the ids in feature-stream")
  (enrich-with-parent-info [persistence features] "returns features enriched with parent info (collection and id)")
  (streams [persistence collection] "returns channel with all stream ids in collection")
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
    (checked (pg/create-schema db (schema dataset))
             (pg/schema-exists? db (schema dataset))))
  (with-bindings {#'dc/*persistence-schema* (schema dataset)}
    (when-not (pg/table-exists? db dc/*persistence-schema* dc/*persistence-features*)
      (checked (do (pg/create-table db dc/*persistence-schema* dc/*persistence-features*
                                    [:id "bigserial" :primary :key]
                                    [:collection "varchar(100)"]
                                    [:feature_id "varchar(50)"]
                                    [:parent_collection "varchar(255)"]
                                    [:parent_id "varchar(50)"]
                                    [:parent_field "varchar(255)"])
                   (pg/create-index db dc/*persistence-schema* dc/*persistence-features* :collection :feature_id)
                   (pg/create-index db dc/*persistence-schema* dc/*persistence-features* :parent_collection :parent_id)
                   (pg/configure-auto-vacuum db dc/*persistence-schema* dc/*persistence-features* 0 10000 0))
               (pg/table-exists? db dc/*persistence-schema* dc/*persistence-features*)))

    (when-not (pg/table-exists? db dc/*persistence-schema* dc/*persistence-feature-stream*)
      (checked (do (pg/create-table db dc/*persistence-schema* dc/*persistence-feature-stream*
                                    [:id "bigserial" :primary :key]
                                    [:version "uuid"]
                                    [:previous_version "uuid"]
                                    [:action "varchar(12)"]
                                    [:collection "varchar(255)"]
                                    [:feature_id "varchar(50)"]
                                    [:validity "timestamp without time zone"]
                                    [:geometry "text"]
                                    [:attributes "text"])
                   (pg/create-index db dc/*persistence-schema* dc/*persistence-feature-stream* :collection :feature_id)
                   (pg/configure-auto-vacuum db dc/*persistence-schema* dc/*persistence-feature-stream* 0 10000 0))
               (pg/table-exists? db dc/*persistence-schema* dc/*persistence-feature-stream*)))

    (when-not (pg/table-exists? db dc/*persistence-schema* dc/*persistence-collections*)
      (checked (do (pg/create-table db dc/*persistence-schema* dc/*persistence-collections*
                                    [:id "bigserial" :primary :key]
                                    [:collection "varchar(255)"]
                                    [:parent_collection "varchar(100)"])
                   (pg/create-index db dc/*persistence-schema* dc/*persistence-collections*
                                    :collection :parent_collection))
               (pg/table-exists? db dc/*persistence-schema* dc/*persistence-collections*)))))

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
                 :current-version (:previous_version record)
                 :validity (:validity record)
                 :geometry (transit/from-json (:geometry record))
                 :attributes (transit/from-json (:attributes record)))]
    (>!! c (persistent! f))))

(defn jdbc-count-records [{:keys [db dataset]} collections]
  (let [query (str "SELECT count(*) as cnt FROM "
                   (qualified-feature-stream dataset) " fs"
                   (when (seq collections) (str " WHERE fs.collection in ("
                                                (clojure.string/join "," (repeat (count collections) "?"))
                                                ")")))
        result (j/with-db-connection [c db]
                                  (j/query c (if-not collections [query] (concat [query] collections))))]
    (:cnt (first result))))

(defn jdbc-get-last-n [{:keys [db dataset] :as pers} n collections]
  (let [n-records (when n (log/debug "Counting rows for" dataset collections)
                          (jdbc-count-records pers collections))
        offset (when n (if (> n n-records) 0 (- n-records n)))
        _ (when offset (log/debug "using offset" offset))
        query (str "SELECT fs.id,
       fs.collection,
       fs.feature_id,
       fs.action,
       fs.version,
       fs.previous_version,
       fs.validity,
       fs.attributes,
       fs.geometry
  FROM " (qualified-feature-stream dataset) " fs "
  (when (seq collections) (str " WHERE fs.collection in ("
                               (clojure.string/join "," (repeat (count collections) "?"))
                               ")"))
  " ORDER BY id ASC"
  (when n (str " OFFSET " offset)))
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

(defn jdbc-get-range [{:keys [db dataset]} start end collections]
  (let [query (str "SELECT fs.id, fs.collection, fs.feature_id, fs.action, fs.version,
                    fs.previous_version, fs.validity, fs.attributes, fs.geometry
                    FROM " (qualified-feature-stream dataset) " fs
                    WHERE fs.id BETWEEN " start " AND " end
                    (when (seq collections) (str " AND fs.collection in ("
                               (clojure.string/join "," (repeat (count collections) "?"))
                               ")"))
                   " ORDER BY id ASC")
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


(defn jdbc-streams [{:keys [db dataset]} collection]
  (let [query (str "SELECT feature_id FROM " (qualified-features dataset)
                   " WHERE collection = ? ORDER by id ASC")
        result-chan (chan)]
    (a/thread
      (try
        (j/with-db-connection [c db]
                              (let [statement (j/prepare-statement
                                                (doto (j/get-connection c) (.setAutoCommit false))
                                                query :fetch-size 10000)]
                                (j/query c [statement collection] :row-fn #(>!! result-chan (:feature_id %)))
                                (close! result-chan)))
        (catch SQLException e
          (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e))
          (throw e))))
    result-chan))

(defn enrich-query [dataset n-features]
  (str "SELECT feature_id, parent_collection, parent_id FROM "
       (qualified-features dataset) " WHERE collection = ? AND feature_id in ("
       (clojure.string/join "," (repeat n-features "?")) ")"))

(defn jdbc-enrich-with-parent-info [{:keys [db dataset]} features]
  (let [enrich-data (volatile! {})
        per-collection (group-by :collection features)]
    (doseq [[collection grouped-features] per-collection]
      (j/with-db-connection [c db]
        (let [result (j/query c (concat [(enrich-query dataset (count grouped-features)) collection] (map :id grouped-features)))]
          (vswap! enrich-data merge (reduce (fn [acc val] (assoc acc [collection (:feature_id val)] val)) {} result)))))
    (map (fn [feature]
           (let [{:keys [parent_collection parent_id]} (get @enrich-data [(:collection feature) (:id feature)])]
             (if (and parent_collection parent_id)
               (-> feature
                   (assoc :parent-collection parent_collection)
                   (assoc :parent-id parent_id))
               feature)))
         features)))

(defn- jdbc-create-stream
  ([{:keys [db dataset]} entries]
   (with-bench t (log/debug "Created streams in" t "ms")
     (try (j/with-db-connection [c db]
            (apply ( partial j/insert! c (qualified-features dataset) :transaction? (:transaction? db)
                             [:collection :feature_id :parent_collection :parent_id :parent_field])
                   entries))
          (catch SQLException e
            (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e))
            (throw e))))))

(defn- jdbc-load-childs-cache [{:keys [db dataset]} parent-collection parent-ids]
  (partitioned
    (fn [ids]
      (try (j/with-db-connection [c db]
                                 (let [results
                                       (j/query c (apply vector (str "SELECT parent_id, collection, feature_id FROM " (qualified-features dataset)
                                                                     " WHERE parent_collection = ? AND parent_id in ("
                                                                     (clojure.string/join "," (repeat (count ids) "?")) ")")
                                                         parent-collection ids))
                                       per-id (group-by :parent_id results)
                                       for-cache (map (fn [[parent-id values]]
                                                        [[parent-collection parent-id]
                                                         (map (juxt :collection :feature_id) values)]) per-id)]
                                   for-cache))
           (catch SQLException e
             (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e))
             (throw e))))
    jdbc-collection-partition-size parent-ids))

(defn- jdbc-load-parent-cache [{:keys [db dataset]} collection ids]
  (partitioned
    (fn [ids-part]
      (try (j/with-db-connection [c db]
                                 (let [results
                                       (j/query c (apply vector (str "SELECT feature_id, parent_collection, parent_id, parent_field FROM "
                                                                     (qualified-features dataset)
                                                                     " WHERE collection = ? AND feature_id in ("
                                                                     (clojure.string/join "," (repeat (count ids-part) "?")) ")")
                                                         collection ids-part) :as-arrays? true)
                                       for-cache (map (fn [[feature-id parent-collection parent-id parent-field]]
                                                        [[collection feature-id] [parent-collection parent-id parent-field]])
                                                      (drop 1 results))]
                                   for-cache))
           (catch SQLException e
             (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e))
             (throw e))))
    jdbc-collection-partition-size ids))

(defn- jdbc-insert
  ([{:keys [db dataset]} entries]
   (with-bench t (log/debug "Inserted" (count entries) "events in" t "ms")
     (try (j/with-db-connection [c db]
            (apply
             (partial j/insert! c (qualified-feature-stream dataset) :transaction? (:transaction? db)
                      [:version :previous_version :action :collection :feature_id :validity :geometry :attributes])
             entries))
          (catch SQLException e
            (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e))
            (throw e))))))

(defn- jdbc-load-cache [{:keys [db dataset]} collection ids]
  (partitioned
    (fn [ids-part]
      (when (seq ids)
        (try (let [results
                   (j/with-db-connection [c db]
                                         (j/query c (apply vector (str "SELECT DISTINCT ON (feature_id)
 collection, feature_id, validity, action, version FROM " (qualified-feature-stream dataset)
                                                                       " WHERE collection = ? and feature_id in (" (clojure.string/join "," (repeat (count ids-part) "?")) ")
   ORDER BY feature_id ASC, id DESC")
                                                           collection ids-part) :as-arrays? true))]
               (map (fn [[collection id validity action version]] [[collection id]
                                                                   [validity (keyword action) version]])
                    (drop 1 results)))
             (catch SQLException e
               (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e))
               (throw e)))))
    jdbc-collection-partition-size ids))

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
  ;(log/trace "Preparing child cache")
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
  ;(log/trace "Preparing parent cache")
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
  ;(log/trace "Start prepare caches")
  (let [{:keys [db stream-cache childs-cache parent-cache]} persistence
        per-collection (group-by #(subvec % 0 1) collection-id)]
    (doseq [[[collection] grouped] per-collection]
      ;(log/trace "Prepare cache for" collection)
      (let [ids (filter (complement nil?) (map #(nth % 1) grouped))]
        ;(log/trace "Load cache")
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
    (let [key-fn   (fn [_ _ _ collection id _ _ _] [collection id])
          value-fn (fn [_ version _ action _ _ validity _ _] [validity action version])
          previous-version (current-version this collection id)
          batched-insert (batched stream-batch stream-batch-size :batch-fn (partial jdbc-insert this))
          cache-batched-insert (with-cache stream-cache batched-insert key-fn value-fn)]
      (cache-batched-insert version previous-version action collection id validity geometry attributes)))
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
  (get-range[this start end collections]
    (jdbc-get-range this start end collections))
  (enrich-with-parent-info [this features]
    (jdbc-enrich-with-parent-info this features))
  (streams [this collection]
    (jdbc-streams this collection))
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
