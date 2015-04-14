(ns pdok.featured.persistence
  (:require [pdok.cache :refer :all]
            [clojure.core.cache :as cache]
            [clojure.java.jdbc :as j]
            [clj-time [coerce :as tc]]
            [cognitect.transit :as transit])
  (:import [java.io ByteArrayOutputStream]))

;; DROP TABLE IF EXISTS featured.feature;

;; CREATE TABLE featured.feature
;; (
;;   id bigserial NOT NULL,
;;   action character(12) NOT NULL,
;;   dataset character varying(100) NOT NULL,
;;   collection character varying(255) NOT NULL,
;;   feature_id character varying(50) NOT NULL,
;;   validity timestamp without time zone NOT NULL,
;;   geometry text NULL,
;;   attributes text NULL,
;;   CONSTRAINT feature_pkey PRIMARY KEY (id)
;; )
;; WITH (
;;   OIDS=FALSE
;; );
;; ALTER TABLE featured.feature
;;   OWNER TO postgres;

;; CREATE INDEX feature_index
;;   ON featured.feature
;;   USING btree
;;   (dataset, collection, id);

;; --DROP TABLE featured.feature_link;

;; CREATE TABLE featured.feature_link
;; (
;;   id bigserial NOT NULL,
;;   dataset character varying(100),
;;   collection character varying(255),
;;   feature_id character varying(50),
;;   parent_collection character varying(255),
;;   parent_id character varying(50),
;;   CONSTRAINT feature_link_pkey PRIMARY KEY (id)
;; )
;; WITH (
;;   OIDS=FALSE
;; );
;; ALTER TABLE featured.feature_link
;;   OWNER TO postgres;

;; -- Index: featured.link_child_index

;; -- DROP INDEX featured.link_child_index;

;; CREATE INDEX link_child_index
;;   ON featured.feature_link
;;   USING btree
;;   (dataset COLLATE pg_catalog."default", collection COLLATE pg_catalog."default", feature_id COLLATE pg_catalog."default");

;; -- Index: featured.link_parent_index

;; -- DROP INDEX featured.link_parent_index;

;; CREATE INDEX link_parent_index
;;   ON featured.feature_link
;;   USING btree
;;   (dataset COLLATE pg_catalog."default", parent_collection COLLATE pg_catalog."default", parent_id COLLATE pg_catalog."default");



(defprotocol ProcessorPersistence
  (init [this])
  (stream-exists? [this dataset collection id])
  (create-stream
    [this dataset collection id]
    [this dataset collection id parent-collection parent-id])
  (append-to-stream [this action dataset collection id validity geometry attributes])
  (current-validity [this dataset collection id])
  (last-action [this dataset collection id])
  (close [_])
  )


(def joda-time-writer
  (transit/write-handler
   (constantly "m")
   (fn [v] (-> v tc/to-date .getTime))
   (fn [v] (-> v tc/to-date .getTime .toString))))

(defn to-json [obj]
  (let [out (ByteArrayOutputStream. 1024)
        writer (transit/writer out :json
                               {:handlers {org.joda.time.DateTime joda-time-writer}})]
    (transit/write writer obj)
    (.toString out))
  )

(defn- jdbc-create-stream
  ([db dataset collection id parent-collection parent-id]
   (jdbc-create-stream db (list [dataset collection id parent-collection parent-id])))
  ([db entries]
   (j/with-db-connection [c db]
     (apply ( partial j/insert! c :featured.feature_link :transaction? false?
                      [:dataset :collection :feature_id :parent_collection :parent_id])
            entries))))


(defn- jdbc-transform-for-db [entry]
  (let [[type dataset collection id validity geometry attributes] entry
        ]
    [(name type) dataset collection id
     (tc/to-timestamp validity)
     (when geometry (to-json geometry))
     (to-json attributes)])
  )

(defn- jdbc-insert
  ([db action dataset collection id validity geometry attributes]
   (jdbc-insert db (list [action dataset collection id validity geometry attributes])))
  ([db entries]
   (try (j/with-db-connection [c db]
          (let [records (map jdbc-transform-for-db entries)]
            (apply
             (partial j/insert! c :featured.feature :transaction? false
                      [:action :dataset :collection :feature_id :validity :geometry :attributes])
             records)
            ))
        (catch java.sql.SQLException e (j/print-sql-exception-chain e)))))

(defn- jdbc-load-cache [db dataset collection]
  (let [results
        (j/with-db-connection [c db]
          (j/query c ["SELECT dataset, collection, feature_id, validity as cur_val, action as last_action FROM (
 SELECT dataset, collection, feature_id, action, validity,
 row_number() OVER (PARTITION BY dataset, collection, feature_id ORDER BY id DESC) AS rn
 FROM featured.feature
 WHERE dataset = ? AND collection = ?) a
WHERE rn = 1"
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
 FROM featured.feature
 WHERE dataset = ? AND collection = ? AND feature_id = ?) a
WHERE rn = 1"
                       dataset collection id] :as-arrays? true)]
       (-> results first))))

(deftype JdbcProcessorPersistence [db]
  ProcessorPersistence
  (init [_])
  (stream-exists? [_ dataset collection id]
    (let [current-validity (partial jdbc-last-stream-validity-and-action db)]
      (not (nil? (current-validity dataset collection id)))))
  (create-stream [this dataset collection id]
    (create-stream this dataset collection id nil nil))
  (create-stream [_ dataset collection id parent-collection parent-id]
    (jdbc-create-stream db dataset collection id parent-collection parent-id))
  (append-to-stream [_ action dataset collection id validity geometry attributes]
    ; compose with nil, because insert returns record. Should fix this...
    (jdbc-insert db action dataset collection id validity geometry attributes)
    nil)
  (current-validity [_ dataset collection id]
    (-> (jdbc-last-stream-validity-and-action db dataset collection id) first))
  (last-action [_ dataset collection id]
    (-> (jdbc-last-stream-validity-and-action db dataset collection id) second))
  (close [_])
  )

(deftype CachedJdbcProcessorPersistence [db stream-batch stream-batch-size stream-cache stream-load-cache?
                                         link-batch link-batch-size]
  ProcessorPersistence
  (init [_])
  (stream-exists? [this dataset collection id]
    (not (nil? (current-validity this dataset collection id))))
  (create-stream [this dataset collection id]
    (create-stream this dataset collection id nil nil))
  (create-stream [_ dataset collection id parent-collection parent-id]
    (let [batched (with-batch link-batch link-batch-size (partial jdbc-create-stream db))]
      (batched dataset collection id parent-collection parent-id)))
  (append-to-stream [_ action dataset collection id validity geometry attributes]
    (let [key-fn   #(->> % (drop 1) (take 3))
          value-fn (fn [params] [(nth params 4) (nth params 0)])
          batched (with-batch stream-batch stream-batch-size (partial jdbc-insert db))
          cache-batched (with-cache stream-cache batched key-fn value-fn)]
      (cache-batched action dataset collection id validity geometry attributes)))
  (current-validity [_ dataset collection id]
    (let [key-fn #(->> % (take 3))
          load-cache (fn [dataset collection id]
                       (when (stream-load-cache? dataset collection) (jdbc-load-cache db dataset collection)))
          cached (use-cache stream-cache key-fn load-cache)]
      (first (cached dataset collection id))))
  (last-action [_ dataset collection id]
    (let [key-fn #(->> % (take 3))
          load-cache (fn [dataset collection id]
                       (when (stream-load-cache? dataset collection) (jdbc-load-cache db dataset collection)))
          cached (use-cache stream-cache key-fn load-cache)]
      (second (cached dataset collection id))))
  (close [_]
    (flush-batch stream-batch (partial jdbc-insert db))
    (flush-batch link-batch (partial jdbc-create-stream db)))
  )

(defn once-true-fn
  "Returns a function which returns true once for an argument"
  []
  (let [mem (atom {})]
    (fn [& args]
      (if-let [e (find @mem args)]
        false
        (do
          (swap! mem assoc args nil)
          true)))))

(defn jdbc-processor-persistence [config]
  (let [db (:db-config config)]
    (JdbcProcessorPersistence. db)))

(defn cached-jdbc-processor-persistence [config]
  (let [db (:db-config config)
        stream-batch-size (or (:stream-batch-size config) (:batch-size config) 100000)
        stream-batch (ref (clojure.lang.PersistentQueue/EMPTY))
        stream-cache (ref (cache/basic-cache-factory {}))
        stream-load-cache? (once-true-fn)
        link-batch-size (or (:link-batch-size config) (:batch-size config) 10000)
        link-batch (ref (clojure.lang.PersistentQueue/EMPTY))]
    (CachedJdbcProcessorPersistence. db stream-batch stream-batch-size stream-cache stream-load-cache?
                                     link-batch link-batch-size)))

;; (def ^:private pgdb {:subprotocol "postgresql"
;;                      :subname "//localhost:5432/pdok"
;;                      :user "postgres"
;;                      :password "postgres"})

;(def pers ( processor-jdbc-persistence {:db-config pgdb}))
;((:append-to-stream pers) "set" "col" "1" (tl/local-now))
;((:shutdown pers))
