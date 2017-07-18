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

(declare prepare-caches)

(def ^Integer jdbc-collection-partition-size 30000)

(defprotocol ProcessorPersistence
  (init [persistence for-dataset])
  (prepare [persistence features] "Called before processing this feature batch")
  (flush [persistence])
  (collections [persistence])
  (create-collection [persistence collection])
  (stream-exists? [persistence collection id])
  (create-stream [persistence collection id])
  (append-to-stream [persistence version action collection id validity attributes])
  (current-validity [persistence collection id])
  (last-action [persistence collection id])
  (current-version [persistence collection id])
  (get-last-n [persistence n collections]
    "Returns channel with n last stream entries for all collections in collections
If n nil => no limit, if collections nil => all collections")
  (streams [persistence collection] "returns channel with all stream ids in collection")
  (close [persistence]))

(defn collection-exists? [persistence collection]
  (let [collections (collections persistence)]
    (seq (filter #(= (:name %) collection) collections))))

(defn schema [dataset]
  (str (name dc/*persistence-schema-prefix*) "_" dataset))

(defn- qualified-features [dataset]
  (str (pg/quoted (schema dataset)) "." (name dc/*persistence-features*)))

(defn- qualified-feature-stream [dataset]
  (str (pg/quoted (schema dataset)) "." (name dc/*persistence-feature-stream*)))

(defn- qualified-persistence-collections [dataset]
  (str (pg/quoted (schema dataset)) "." (name dc/*persistence-collections*)))

(defn- jdbc-init [tx dataset]
  (when-not (pg/schema-exists? tx (schema dataset))
    (checked (pg/create-schema tx (schema dataset))
             (pg/schema-exists? tx (schema dataset))))
  (with-bindings {#'dc/*persistence-schema* (schema dataset)}
    (when-not (pg/table-exists? tx dc/*persistence-schema* dc/*persistence-features*)
      (checked (do (pg/create-table tx dc/*persistence-schema* dc/*persistence-features*
                                    [:id "bigserial" :primary :key]
                                    [:collection "varchar(255)"]
                                    [:feature_id "varchar(255)"])
                   (pg/create-index tx dc/*persistence-schema* dc/*persistence-features* :collection :feature_id)
                   (pg/configure-auto-vacuum tx dc/*persistence-schema* dc/*persistence-features* 0 10000 0))
               (pg/table-exists? tx dc/*persistence-schema* dc/*persistence-features*)))

    (when-not (pg/table-exists? tx dc/*persistence-schema* dc/*persistence-feature-stream*)
      (checked (do (pg/create-table tx dc/*persistence-schema* dc/*persistence-feature-stream*
                                    [:id "bigserial" :primary :key]
                                    [:version "uuid"]
                                    [:previous_version "uuid"]
                                    [:action "varchar(12)"]
                                    [:collection "varchar(255)"]
                                    [:feature_id "varchar(255)"]
                                    [:validity "timestamp without time zone"]
                                    [:attributes "text"])
                   (pg/create-index tx dc/*persistence-schema* dc/*persistence-feature-stream* :collection :feature_id)
                   (pg/configure-auto-vacuum tx dc/*persistence-schema* dc/*persistence-feature-stream* 0 10000 0))
               (pg/table-exists? tx dc/*persistence-schema* dc/*persistence-feature-stream*)))

    (when-not (pg/table-exists? tx dc/*persistence-schema* dc/*persistence-collections*)
      (checked (do (pg/create-table tx dc/*persistence-schema* dc/*persistence-collections*
                                    [:id "bigserial" :primary :key]
                                    [:collection "varchar(255)"])
                   (pg/create-index tx dc/*persistence-schema* dc/*persistence-collections* :collection))
               (pg/table-exists? tx dc/*persistence-schema* dc/*persistence-collections*)))))

(defn jdbc-create-collection [tx dataset collection]
  (pg/insert tx (qualified-persistence-collections dataset) [:collection] [collection]))

(defn jdbc-all-collections [tx dataset]
  (let [query (str "SELECT collection AS name FROM " (qualified-persistence-collections dataset))]
    (j/query tx [query])))

(defn record->feature [c record]
  (let [f (transient {})
        f (assoc! f
                 :collection (:collection record)
                 :id (:feature_id record)
                 :action (keyword (:action record))
                 :version (:version record)
                 :current-version (:previous_version record)
                 :validity (:validity record)
                 :attributes (transit/from-json (:attributes record)))]
    (>!! c (persistent! f))))

(defn jdbc-count-records [tx dataset collections]
  (let [query (str "SELECT COUNT(*) AS cnt "
                   "FROM " (qualified-feature-stream dataset) " fs "
                   (when (seq collections) (str "WHERE fs.collection IN ("
                                                (clojure.string/join "," (repeat (count collections) "?"))
                                                ")")))
        result (j/query tx (cons query collections))]
    (:cnt (first result))))

(defn jdbc-get-last-n [db tx dataset n collections]
  (let [n-records (when n (log/debug "Counting rows for" dataset collections)
                          (jdbc-count-records tx dataset collections))
        offset (when n (if (> n n-records) 0 (- n-records n)))
        _ (when offset (log/debug "using offset" offset))
        query (str "SELECT fs.id, fs.collection, fs.feature_id, fs.action, fs.version, fs.previous_version,"
                   "fs.validity, fs.attributes "
                   "FROM " (qualified-feature-stream dataset) " fs "
                   (when (seq collections) (str "WHERE fs.collection IN ("
                                                (clojure.string/join "," (repeat (count collections) "?"))
                                                ")"))
                   "ORDER BY id ASC "
                   (when n (str "OFFSET " offset)))
        result-chan (chan)]
    (a/thread
      (j/with-db-connection [c db] ;; Cannot use tx from a different thread
        (let [statement (j/prepare-statement
                         (doto (j/get-connection c) (.setAutoCommit false))
                         query :fetch-size 10000)]
          (j/query c (cons statement collections) :row-fn (partial record->feature result-chan))
          (close! result-chan))))
    result-chan))

(defn jdbc-streams [db dataset collection]
  (let [query (str "SELECT feature_id FROM " (qualified-features dataset) " WHERE collection = ? ORDER BY id ASC")
        result-chan (chan)]
    (a/thread
      (try (j/with-db-connection [c db] ;; Cannot use tx from a different thread
             (let [statement (j/prepare-statement
                               (doto (j/get-connection c) (.setAutoCommit false))
                               query :fetch-size 10000)]
               (j/query c [statement collection] :row-fn #(>!! result-chan (:feature_id %)))
               (close! result-chan)))
           (catch SQLException e
             (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e))
             (throw e))))
    result-chan))

(defn- jdbc-create-stream [tx dataset entries]
  (with-bench t (log/debug "Created streams in" t "ms")
    (try
      (pg/batch-insert tx (qualified-features dataset) [:collection :feature_id] entries)
      (catch SQLException e
        (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e))
        (throw e)))))

(defn- jdbc-insert [tx dataset entries]
  (let [entries (map (fn [entry] (-> entry vec (update 6 transit/to-json))) entries)]
    (with-bench t (log/debug "Inserted" (count entries) "events in" t "ms")
      (try
        (pg/batch-insert tx (qualified-feature-stream dataset)
                         [:version :previous_version :action :collection :feature_id :validity :attributes] entries)
        (catch SQLException e
          (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e))
          (throw e))))))

(defn- jdbc-load-cache [tx dataset collection ids]
  (partitioned
    (fn [ids-part]
      (when (seq ids)
        (try
          (let [results (j/query tx (apply vector (str "SELECT DISTINCT ON (feature_id) "
                                                       "collection, feature_id, validity, action, version "
                                                       "FROM " (qualified-feature-stream dataset) " "
                                                       "WHERE collection = ? AND feature_id IN ("
                                                       (clojure.string/join "," (repeat (count ids-part) "?")) ")"
                                                       "ORDER BY feature_id ASC, id DESC")
                                           collection ids-part) :as-arrays? true)]
            (map (fn [[collection id validity action version]] [[collection id] [validity (keyword action) version]])
                 (drop 1 results)))
          (catch SQLException e
            (log/with-logs ['pdok.featured.persistence :error :error] (j/print-sql-exception-chain e))
            (throw e)))))
    jdbc-collection-partition-size ids))

(defn- current-state [persistence collection id]
  (let [key-fn (fn [collection id] [collection id])
        cached (use-cache (:stream-cache persistence) key-fn)]
      (cached collection id)))

(defn prepare-caches [persistence collection-id]
  ;(log/trace "Start prepare caches")
  (let [{:keys [stream-cache tx dataset]} persistence
        per-collection (group-by #(subvec % 0 1) collection-id)]
    (doseq [[[collection] grouped] per-collection]
      ;(log/trace "Prepare cache for" collection)
      (let [ids (filter (complement nil?) (map #(nth % 1) grouped))]
        ;(log/trace "Load cache")
        (apply-to-cache stream-cache (jdbc-load-cache tx dataset collection ids))))))

(defrecord CachedJdbcProcessorPersistence [db tx dataset collections-cache stream-batch stream-batch-size stream-cache
                                           link-batch link-batch-size]
  ProcessorPersistence
  (init [this for-dataset]
    (let [transaction (pg/begin-transaction db)
          inited (-> this (assoc :dataset for-dataset) (assoc :tx transaction))
          _ (jdbc-init transaction for-dataset)
          _ (vreset! collections-cache (into #{} (jdbc-all-collections transaction for-dataset)))]
      inited))
  (prepare [this features]
    (with-bench t (log/debug "Prepared cache in" t "ms")
      (prepare-caches this (map (juxt :collection :id) features)))
    this)
  (flush [this]
    (flush-batch stream-batch (partial jdbc-insert tx dataset))
    (flush-batch link-batch (partial jdbc-create-stream tx dataset))
    (dosync
     (vreset! stream-cache (cache/basic-cache-factory {})))
    this)
  (collections [_]
    @collections-cache)
  (create-collection [this collection]
    (jdbc-create-collection tx dataset collection)
    (vswap! collections-cache conj {:name collection}))
  (stream-exists? [this collection id]
    (not (nil? (last-action this collection id))))
  (create-stream [this collection id]
    (let [batched-create (batched link-batch link-batch-size :batch-fn (partial jdbc-create-stream tx dataset))]
      (when-not (collection-exists? this collection)
        (create-collection this collection))
      (batched-create collection id)))
  (append-to-stream [this version action collection id validity attributes]
    (let [key-fn   (fn [_ _ _ collection id _ _] [collection id])
          value-fn (fn [_ version _ action _ _ validity _] [validity action version])
          previous-version (current-version this collection id)
          batched-insert (batched stream-batch stream-batch-size :batch-fn (partial jdbc-insert tx dataset))
          cache-batched-insert (with-cache stream-cache batched-insert key-fn value-fn)]
      (cache-batched-insert version previous-version action collection id validity attributes)))
  (current-validity [this collection id]
    (get (current-state this collection id) 0))
  (last-action [this collection id]
    (get (current-state this collection id) 1))
  (current-version [this collection id]
    (get (current-state this collection id) 2))
  (get-last-n [this n collections]
    (jdbc-get-last-n db tx dataset n collections))
  (streams [this collection]
    (jdbc-streams db dataset collection))
  (close [this]
    (flush this)
    (pg/commit-transaction tx)))

(defn make-cached-jdbc-processor-persistence [config]
  (let [db (:db-config config)
        collections-cache (volatile! #{})
        stream-batch-size (or (:stream-batch-size config) (:batch-size config) 10000)
        stream-batch (volatile! (PersistentQueue/EMPTY))
        stream-cache (volatile! (cache/basic-cache-factory {}))
        link-batch-size (or (:link-batch-size config) (:batch-size config) 10000)
        link-batch (volatile! (PersistentQueue/EMPTY))]
    (CachedJdbcProcessorPersistence. db nil "unknown-dataset" collections-cache stream-batch stream-batch-size
                                     stream-cache link-batch link-batch-size)))

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
  (create-collection [persistence collection]
    nil)
  (stream-exists? [persistence collection id]
    false)
  (create-stream [persistence collection id]
    nil)
  (append-to-stream [persistence version action collection id validity attributes]
    nil)
  (current-validity [persistence collection id]
    nil)
  (last-action [persistence collection id]
    nil)
  (current-version [persistence collection id]
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
