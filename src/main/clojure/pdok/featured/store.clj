(ns pdok.featured.store
  (:require [pdok.featured.feature]
            [clojure.java.jdbc :as j]
            [clj-time.coerce :as tc]
            [environ.core :refer [env]])
  (:import  [pdok.featured.feature NewFeature ChangeFeature CloseFeature DeleteFeature]))

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

(def pgdb {:subprotocol "postgresql"
           :subname (or (env :database-url) "//localhost:5432/pdok")
           :user (or (env :database-user) "postgres")
           :password (or (env :database-password) "postgres")})

(defn jdbc-insert [db dataset collection id validity]
  (j/with-db-connection [c db]
    (j/insert! c :featured.feature
               {:dataset dataset
                :collection collection
                :feature_id id
                :validity (-> validity tc/to-timestamp)})))

(defn jdbc-stream-validity [db dataset collection id]
  (j/with-db-connection [c db]
    (let [results
          (j/query c ["SELECT max(validity) FROM featured.feature
                       WHERE dataset = ? AND collection = ? AND feature_id = ?"
                      dataset collection id])]
      (-> results first :max))))

(defn jdbc-stream-exists? [db dataset collection id]
  (not (nil? (jdbc-stream-validity db dataset collection id)) ))

(defn make-jdbc-persistence
  ([] (make-jdbc-persistence {:db-config pgdb}))
  ([config]
   (let [db (:db-config config)
         create-stream (fn [dataset collection id])
         stream-exists? (partial jdbc-stream-exists? db)
         ; compose with nil, because insert returns record. Should fix this...
         append-to-stream (comp (fn [_] nil) (partial jdbc-insert db))
         current-validity (partial jdbc-stream-validity db)]
     {:create-stream create-stream
      :stream-exists? stream-exists?
      :append-to-stream append-to-stream
      :current-validity current-validity})))

; TODO Caching? Cache is last feature, key is combined? Misschien vector als key, zo mooi zijn als dat werkt


(defn- process-new-feature [persistence {:keys [ dataset collection id validity]}]
  (if (some nil? [dataset collection id validity])
    "NewFeature requires: dataset collection id validity")
  (let [exists? (:stream-exists? persistence)
        create  (:create-stream persistence)
        append  (:append-to-stream persistence)]
    (if (exists? dataset collection id)
      (str "Stream already exists: " dataset ", " collection ", " id)
      (do (create dataset collection id)
          (append dataset collection id validity)))))

(defn process [{:keys [persistence]} feature]
  "Processes feature event. Returns nil or error reason"
  (if-not persistence "persistence needed")
  (condp instance? feature
    NewFeature (process-new-feature persistence feature)
    (str "Cannot process: " feature)))

(defn make-feature-store
  ([] (let [jdbc-persistence (make-jdbc-persistence)]
        (make-feature-store jdbc-persistence)))
  ([persistence]
   {:persistence persistence}))
