(ns pdok.featured.persistence
  (:require [clojure.java.jdbc :as j]
            [clj-time [coerce :as tc]]
            [clojure.core.cache :as cache]))

;; DROP TABLE IF EXISTS featured.feature;

;; CREATE TABLE featured.feature
;; (
;;   id bigserial NOT NULL,
;;   dataset character varying(100) NOT NULL,
;;   collection character varying(255) NOT NULL,
;;   feature_id character varying(50) NOT NULL,
;;   validity timestamp without time zone NOT NULL,
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

(defn- jdbc-flush [db batch]
  ;(println "flushing")
  (def records nil)
  (dosync
   (def records (map identity @batch))
   (ref-set batch clojure.lang.PersistentQueue/EMPTY))
  (when (not-empty records)
    (j/with-db-connection [c db]
      (apply
       (partial j/insert! c :featured.feature [:dataset :collection :feature_id :validity])
       records))))

(defn- jdbc-insert [db cache batch max-batch-size dataset collection id validity]
  (dosync
   (alter cache #(cache/miss % [dataset collection id] validity))
   (alter batch #(conj % [dataset collection id (-> validity tc/to-timestamp)])))
  (if (<= max-batch-size (count @batch))
    (jdbc-flush db batch)))

(defn- cached-stream-validity [cache dataset collection id]
  (cache/lookup @cache [dataset collection id]))

(defn- jdbc-load-cache* [db cache dataset collection]
  (let [results
        (j/with-db-connection [c db]
          (j/query c ["SELECT dataset, collection, feature_id, max(validity) as cur_val FROM featured.feature
WHERE dataset = ? AND collection = ?
GROUP BY dataset, collection, feature_id"
                      dataset collection]))]
    (dosync
     (doseq [f results]
       (alter cache #(cache/miss % [(:dataset f) (:collection f) (:feature_id f)] (:cur_val f)))))
    )
  )

; sort of hacky? To prevent executing every time, memoize the function
(def ^:private jdbc-load-cache (memoize jdbc-load-cache*))

(defn- jdbc-stream-validity [db cache dataset collection id]
  (let [cached (cached-stream-validity cache dataset collection id)]
    (if cached
      cached
      (do (jdbc-load-cache db cache dataset collection)
          (cached-stream-validity cache dataset collection id))
      )))

(defn- jdbc-stream-exists? [db cache dataset collection id]
  (not (nil? (jdbc-stream-validity db cache dataset collection id)) ))

(defn processor-jdbc-persistence
  ([config]
   (let [db (:db-config config)
         max-batch-size (or (:cache-size config) 10000)
         batch (ref (clojure.lang.PersistentQueue/EMPTY))
         cache (ref (cache/basic-cache-factory {}))
         create-stream (fn [dataset collection id])
         stream-exists? (partial jdbc-stream-exists? db cache)
         ; compose with nil, because insert returns record. Should fix this...
         append-to-stream (comp (fn [_] nil) (partial jdbc-insert db cache batch max-batch-size))
         current-validity (partial jdbc-stream-validity db cache)
         shutdown (partial jdbc-flush db batch)]
     {:init #()
      :create-stream create-stream
      :stream-exists? stream-exists?
      :append-to-stream append-to-stream
      :current-validity current-validity
      :shutdown shutdown })))
