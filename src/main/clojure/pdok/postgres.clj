(ns pdok.postgres
  (:require [clojure.java.jdbc :as j]
            [clj-time [coerce :as tc]]
            [clojure.tools.logging :as log]
            [pdok.transit :as transit]
            [pdok.featured.feature :as f])
  (:import [com.vividsolutions.jts.io WKTWriter]
           [java.util Calendar TimeZone]
           [org.joda.time DateTimeZone LocalDate LocalDateTime]
           (java.sql Types)))

(def wkt-writer (WKTWriter.))

(def utcCal (Calendar/getInstance (TimeZone/getTimeZone "UTC")))

;; The server's time zone, not necessarily Europe/Amsterdam.
;; Used for reading DateTimes, because PostgreSQL returns Z values and we want local ones without time zone.
(def serverTimeZone (DateTimeZone/getDefault))

(extend-protocol j/ISQLValue
  org.joda.time.DateTime
  (sql-value [v] (tc/to-sql-time v))
  org.joda.time.LocalDateTime
  (sql-value [v] (tc/to-sql-time v))
  org.joda.time.LocalDate
  (sql-value [v] (tc/to-sql-date v))
  pdok.featured.GeometryAttribute
  (sql-value [v] (-> v f/as-jts j/sql-value))
  com.vividsolutions.jts.geom.Geometry
  (sql-value [v] (str "SRID=" (.getSRID v) ";" (.write ^WKTWriter wkt-writer v)))
  clojure.lang.Keyword
  (sql-value [v] (name v))
  clojure.lang.IPersistentMap
  (sql-value [v] (transit/to-json v)))

(deftype NilType [clazz]
  j/ISQLValue
  (sql-value [_] nil)
  clojure.lang.IMeta
  (meta [_] {:type clazz}))

(declare clj-to-pg-type)

(defn write-vector [v ^java.sql.PreparedStatement s ^long i]
  (if (empty? v)
    (.setObject s i nil Types/ARRAY)
    (let [con (.getConnection s)
          pg-type (clj-to-pg-type (first v))
          postgres-array (.createArrayOf con pg-type (into-array v))]
      (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/ISQLParameter
  pdok.featured.NilAttribute
  (set-parameter [^pdok.featured.NilAttribute v ^java.sql.PreparedStatement s ^long i]
    (j/set-parameter (NilType. (.-clazz v)) s i))
  org.joda.time.LocalDateTime
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (.setTimestamp s i (j/sql-value v) utcCal))
  org.joda.time.LocalDate
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (.setDate s i (j/sql-value v) utcCal))
  java.util.UUID
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) Types/OTHER))
  pdok.featured.GeometryAttribute
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) Types/OTHER))
  com.vividsolutions.jts.geom.Geometry
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) Types/OTHER))
  clojure.lang.IPersistentMap
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) Types/VARCHAR))
  clojure.lang.IPersistentVector
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (write-vector v s i))
  clojure.lang.PersistentVector
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (write-vector v s i))
  clojure.lang.IPersistentList
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (if (empty? v)
      (.setObject s i nil Types/OTHER)
      (j/set-parameter (into-array v) s i)))
  clojure.lang.IPersistentSet
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (j/set-parameter (into [] v) s i)))

(extend-protocol j/ISQLParameter
  (Class/forName "[Ljava.lang.Long;")
   (set-parameter [v ^java.sql.PreparedStatement s ^long i]
      (let [con (.getConnection s)
            postgres-array (.createArrayOf con "integer" v)]
        (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/ISQLParameter
  (Class/forName "[Ljava.lang.Integer;")
   (set-parameter [v ^java.sql.PreparedStatement s ^long i]
      (let [con (.getConnection s)
            postgres-array (.createArrayOf con "integer" v)]
        (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/ISQLParameter
  (Class/forName "[Ljava.util.UUID;")
   (set-parameter [v ^java.sql.PreparedStatement s ^long i]
      (let [con (.getConnection s)
            postgres-array (.createArrayOf con "uuid" v)]
        (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/ISQLParameter
  (Class/forName "[Ljava.lang.String;")
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (let [con (.getConnection s)
          postgres-array (.createArrayOf con "text" v)]
      (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/IResultSetReadColumn
  java.sql.Date
  (result-set-read-column [v _ _] (LocalDate. ^java.sql.Date v ^DateTimeZone serverTimeZone))
  java.sql.Timestamp
  (result-set-read-column [v _ _] (LocalDateTime. ^java.sql.Timestamp v ^DateTimeZone serverTimeZone))
  java.sql.Array
  (result-set-read-column [v _ _]
    (into [] (.getArray v))))

(def geometry-type "geometry")

(declare clj-to-pg-type)

(defn vector->pg-type [v]
  (let [e (first v)]
    (str (clj-to-pg-type e) "[]")))

(defn clj-to-pg-type [clj-value]
  (let [clj-type (type clj-value)]
    (condp = clj-type
      nil "text"
      pdok.featured.NilAttribute (clj-to-pg-type (NilType. (.-clazz clj-value)))
      clojure.lang.Keyword "text"
      clojure.lang.IPersistentMap "text"
      clojure.lang.IPersistentVector (vector->pg-type clj-value)
      clojure.lang.PersistentVector (vector->pg-type clj-value)
      org.joda.time.DateTime "timestamp with time zone"
      org.joda.time.LocalDateTime "timestamp without time zone"
      org.joda.time.LocalDate "date"
      java.lang.Integer "integer"
      java.lang.Double "double precision"
      java.lang.Boolean "boolean"
      java.util.UUID "uuid"
      pdok.featured.GeometryAttribute geometry-type
      "text")))

(def quoted (j/quoted \"))

(defn begin-transaction [db]
  (let [connection (j/get-connection db)
        _ (.setAutoCommit connection false)]
    (j/add-connection db connection)))

(defn commit-transaction [tx]
  (with-open [connection (j/get-connection tx)]
    (.commit connection)))

(defn schema-exists? [tx schema]
  (let [query "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?"
        results (j/query tx [query schema])]
    (not (nil? (first results)))))

(defn create-schema [tx schema]
  "Create schema"
  (j/execute! tx [(str "CREATE SCHEMA " (-> schema name quoted))]))

(defn table-exists? [tx schema table]
  (let [query "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?"
        results (j/query tx [query schema table])]
    (not (nil? (first results)))))

(defn create-table [tx schema table & fields]
  (j/execute! tx [(apply j/create-table-ddl (str (-> schema name quoted) "." (-> table name quoted)) fields)]))

(defn- remove-underscores [name]
  (if (= (first name) \_)
    (recur (next name))
    (apply str name)))

(defn- create-index* [tx schema table index-type & columns]
  (let [column-names (map name columns)
        quoted-columns (clojure.string/join "," (map quoted column-names))
        index-name (str (name table) "_" (clojure.string/join "_" (map remove-underscores column-names))
                        (or (index-type {:gist "_sidx"}) "_idx"))]
    (try
      (j/execute! tx [(str "CREATE INDEX " (quoted index-name)
                           " ON " (-> schema name quoted) "." (-> table name quoted)
                           " USING " (name index-type) " (" quoted-columns ")")])
      (catch java.sql.SQLException e
        (log/with-logs ['pdok.postgres :error :error] (j/print-sql-exception-chain e))
        (throw e)))))

(defn create-index [tx schema table & columns]
  (apply create-index* tx schema table :btree columns))

(defn kv->clause [[k v]]
  (if v
    (str (name k) " = '" (clojure.string/replace v #"'" "''") "'")
    (str (name k) " is null")))

(defn map->where-clause
  ([clauses] (clojure.string/join " AND " (map kv->clause clauses)))
  ([clauses table] (clojure.string/join " AND " (map #(str table "." (kv->clause %)) clauses))))

(defn configure-auto-vacuum [tx schema table vacuum-scale-factor vacuum-threshold analyze-scale-factor]
  (try
    (do
      (j/execute! tx [(str "ALTER TABLE " (-> schema name quoted) "." (-> table name quoted)
                           " SET (autovacuum_vacuum_scale_factor = " vacuum-scale-factor ")")])
      (j/execute! tx [(str "ALTER TABLE " (-> schema name quoted) "." (-> table name quoted)
                           " SET (autovacuum_vacuum_threshold = " vacuum-threshold ")")])
      (j/execute! tx [(str "ALTER TABLE " (-> schema name quoted) "." (-> table name quoted)
                           " SET (autovacuum_analyze_scale_factor = " analyze-scale-factor ")")]))
    (catch java.sql.SQLException e
      (log/with-logs ['pdok.postgres :error :error] (j/print-sql-exception-chain e))
      (throw e))))
