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
;;   action character(6) NOT NULL,
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

(defprotocol ProcessorPersistence
  (init [this])
  (stream-exists? [this dataset collection id])
  (create-stream [this dataset collection id])
  (append-to-stream [this type dataset collection id validity geometry attributes])
  (current-validity [this dataset collection id])
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

(defn jdbc-transform-for-db [entry]
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
   (j/with-db-connection [c db]
     (let [records (map jdbc-transform-for-db entries)]
       (apply
        (partial j/insert! c :featured.feature :transaction? false
                 [:action :dataset :collection :feature_id :validity :geometry :attributes])
        records)
       ))))

(defn- jdbc-load-cache [db dataset collection]
  (let [results
        (j/with-db-connection [c db]
          (j/query c ["SELECT dataset, collection, feature_id, max(validity) as cur_val FROM featured.feature
WHERE dataset = ? AND collection = ?
GROUP BY dataset, collection, feature_id"
                      dataset collection]))]
    (map (fn [f] [[(:dataset f) (:collection f) (:feature_id f)] (:cur_val f)]) results)
    )
  )

(defn- jdbc-stream-validity [db dataset collection id]
   (j/with-db-connection [c db]
     (let [results
           (j/query c ["SELECT max(validity) FROM featured.feature
                        WHERE dataset = ? AND collection = ? AND feature_id = ?"
                       dataset collection id])]
       (-> results first :max))))

(deftype JdbcProcessorPersistence [db]
  ProcessorPersistence
  (init [_])
  (stream-exists? [_ dataset collection id]
    (let [current-validity (partial jdbc-stream-validity db)]
      (not (nil? (current-validity dataset collection id)))))
  (create-stream [_ dataset collection id])
  (append-to-stream [_ type dataset collection id validity geometry attributes]
    ; compose with nil, because insert returns record. Should fix this...
    (jdbc-insert db type dataset collection id validity geometry attributes)
    nil)
  (current-validity [_ dataset collection id]
    (jdbc-stream-validity db dataset collection id))
  (close [_])
  )

(deftype CachedJdbcProcessorPersistence [db batch batch-size cache load-cache?]
  ProcessorPersistence
  (init [_])
  (stream-exists? [this dataset collection id]
    (not (nil? (current-validity this dataset collection id))))
  (create-stream [_ dataset collection id])
  (append-to-stream [_ type dataset collection id validity geometry attributes]
    (let [key-fn   #(->> % (drop 1) (take 3))
          value-fn #(->> % (drop 4) (take 1) first)
          batched (with-batch batch batch-size (partial jdbc-insert db))
          cache-batched (with-cache cache batched key-fn value-fn)]
      (cache-batched type dataset collection id validity geometry attributes)))
  (current-validity [_ dataset collection id]
    (let [key-fn #(->> % (take 3))
          load-cache (fn [dataset collection id]
                       (when (load-cache? dataset collection) (jdbc-load-cache db dataset collection)))
          cached (use-cache cache key-fn load-cache)]
      (cached dataset collection id)))
  (close [_]
    (flush-batch batch (partial jdbc-insert db)))
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
        batch-size (or (:batch-size config) 100000)
        batch (ref (clojure.lang.PersistentQueue/EMPTY))
        cache (ref (cache/basic-cache-factory {}))
        load-cache? (once-true-fn)]
    (CachedJdbcProcessorPersistence. db batch batch-size cache load-cache?)))

;; (def ^:private pgdb {:subprotocol "postgresql"
;;                      :subname "//localhost:5432/pdok"
;;                      :user "postgres"
;;                      :password "postgres"})

;(def pers ( processor-jdbc-persistence {:db-config pgdb}))
;((:append-to-stream pers) "set" "col" "1" (tl/local-now))
;((:shutdown pers))
