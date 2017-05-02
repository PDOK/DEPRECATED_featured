(ns pdok.postgres
  (:require [clojure.java.jdbc :as j]
            [clj-time [coerce :as tc]]
            [clojure.tools.logging :as log]
            [pdok.transit :as transit]
            [pdok.featured.feature :as f]
            [clojure.string :as str])
  (:import (clojure.lang IMeta IPersistentList IPersistentMap IPersistentSet IPersistentVector Keyword PersistentVector)
           (com.vividsolutions.jts.geom Geometry)
           (com.vividsolutions.jts.io WKTWriter)
           (java.sql Array Connection Date PreparedStatement SQLException Timestamp Types)
           (java.util Calendar TimeZone UUID)
           (org.joda.time DateTime DateTimeZone LocalDate LocalDateTime)
           (pdok.featured GeometryAttribute NilAttribute)))

(def wkt-writer (WKTWriter.))

(def utcCal (Calendar/getInstance (TimeZone/getTimeZone "UTC")))

;; The server's time zone, not necessarily Europe/Amsterdam.
;; Used for reading DateTimes, because PostgreSQL returns Z values and we want local ones without time zone.
(def serverTimeZone (DateTimeZone/getDefault))

(extend-protocol j/ISQLValue
  DateTime
  (sql-value [v] (tc/to-sql-time v))
  LocalDateTime
  (sql-value [v] (tc/to-sql-time v))
  LocalDate
  (sql-value [v] (tc/to-sql-date v))
  GeometryAttribute
  (sql-value [v] (-> v f/as-jts j/sql-value))
  Geometry
  (sql-value [v] (str "SRID=" (.getSRID v) ";" (.write ^WKTWriter wkt-writer v)))
  Keyword
  (sql-value [v] (name v))
  IPersistentMap
  (sql-value [v] (transit/to-json v)))

(deftype NilType [clazz]
  j/ISQLValue
  (sql-value [_] nil)
  IMeta
  (meta [_] {:type clazz}))

(declare clj-to-pg-type)

(defn write-vector [v ^PreparedStatement s ^long i]
  (if (empty? v)
    (.setObject s i nil Types/ARRAY)
    (let [con (.getConnection s)
          pg-type (clj-to-pg-type (first v))
          postgres-array (.createArrayOf con pg-type (into-array v))]
      (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/ISQLParameter
  NilAttribute
  (set-parameter [^NilAttribute v ^PreparedStatement s ^long i]
    (j/set-parameter (NilType. (.-clazz v)) s i))
  LocalDateTime
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setTimestamp s i (j/sql-value v) utcCal))
  LocalDate
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setDate s i (j/sql-value v) utcCal))
  UUID
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) Types/OTHER))
  GeometryAttribute
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) Types/OTHER))
  Geometry
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) Types/OTHER))
  IPersistentMap
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) Types/VARCHAR))
  IPersistentVector
  (set-parameter [v ^PreparedStatement s ^long i]
    (write-vector v s i))
  PersistentVector
  (set-parameter [v ^PreparedStatement s ^long i]
    (write-vector v s i))
  IPersistentList
  (set-parameter [v ^PreparedStatement s ^long i]
    (if (empty? v)
      (.setObject s i nil Types/OTHER)
      (j/set-parameter (into-array v) s i)))
  IPersistentSet
  (set-parameter [v ^PreparedStatement s ^long i]
    (j/set-parameter (into [] v) s i)))

(extend-protocol j/ISQLParameter
  (Class/forName "[Ljava.lang.Long;")
  (set-parameter [v ^PreparedStatement s ^long i]
    (let [con (.getConnection s)
          postgres-array (.createArrayOf con "integer" v)]
      (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/ISQLParameter
  (Class/forName "[Ljava.lang.Integer;")
  (set-parameter [v ^PreparedStatement s ^long i]
    (let [con (.getConnection s)
          postgres-array (.createArrayOf con "integer" v)]
      (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/ISQLParameter
  (Class/forName "[Ljava.util.UUID;")
  (set-parameter [v ^PreparedStatement s ^long i]
    (let [con (.getConnection s)
          postgres-array (.createArrayOf con "uuid" v)]
      (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/ISQLParameter
  (Class/forName "[Ljava.lang.String;")
  (set-parameter [v ^PreparedStatement s ^long i]
    (let [con (.getConnection s)
          postgres-array (.createArrayOf con "text" v)]
      (.setObject s i postgres-array Types/ARRAY))))

(extend-protocol j/IResultSetReadColumn
  Date
  (result-set-read-column [v _ _]
    (LocalDate. ^Date v ^DateTimeZone serverTimeZone))
  Timestamp
  (result-set-read-column [v _ _]
    (LocalDateTime. ^Timestamp v ^DateTimeZone serverTimeZone))
  Array
  (result-set-read-column [v _ _]
    (into [] (.getArray v))))

(def geometry-type "geometry")

(defn vector->pg-type [v]
  (let [e (first v)]
    (str (clj-to-pg-type e) "[]")))

(defn clj-to-pg-type [clj-value]
  (let [clj-type (type clj-value)]
    (condp = clj-type
      nil "text"
      NilAttribute (clj-to-pg-type (NilType. (.-clazz clj-value)))
      Keyword "text"
      IPersistentMap "text"
      IPersistentVector (vector->pg-type clj-value)
      PersistentVector (vector->pg-type clj-value)
      DateTime "timestamp with time zone"
      LocalDateTime "timestamp without time zone"
      LocalDate "date"
      Integer "integer"
      Double "double precision"
      Boolean "boolean"
      UUID "uuid"
      GeometryAttribute geometry-type
      "text")))

(def quoted (j/quoted \"))

(defn sql-identifier [s]
  (j/quoted \" (name s)))

(defn qualified-table [schema table]
  (str (sql-identifier schema) "." (sql-identifier table)))

(defn begin-transaction [db]
  (let [connection (j/get-connection db)
        _ (.setAutoCommit connection false)]
    {:connection connection}))

(defn commit-transaction [tx]
  (with-open [connection (j/get-connection tx)]
    (.commit connection)))

(defn execute-batch-query [tx ^String query batch]
  (with-open [stmt (let [^Connection c (:connection tx)] (.prepareStatement c query))]
    (doseq [values batch]
      (doseq [value (map-indexed vector values)]
        (j/set-parameter
          (second value)
          ^PreparedStatement stmt
          ^Integer (-> value first inc)))
      (.addBatch ^PreparedStatement stmt))
    (.executeBatch ^PreparedStatement stmt)))

(defn batch-insert [tx qualified-table columns batch]
  (let [query (str "INSERT INTO " qualified-table
                   "(" (->> columns (map sql-identifier) (str/join ", ")) ")"
                   " VALUES (" (->> columns (map (constantly "?")) (str/join ", ")) ")")]
    (execute-batch-query tx query batch)))

(defn batch-delete [tx qualified-table columns batch]
  (let [query (str "DELETE FROM " qualified-table
                   " WHERE " (->> columns (map sql-identifier) (map #(str % " = ?")) (str/join " AND ")))]
    (execute-batch-query tx query batch)))

(defn execute-query
  ([tx ^String query]
   (execute-query tx query []))
  ([tx ^String query values]
   (with-open [stmt (let [^Connection c (:connection tx)] (.prepareStatement c query))]
     (doseq [value (map-indexed vector values)]
       (j/set-parameter
         (second value)
         ^PreparedStatement stmt
         ^Integer (-> value first inc)))
     (.execute ^PreparedStatement stmt))))

(defn insert [tx qualified-table columns values]
  (let [query (str "INSERT INTO " qualified-table
                   "(" (->> columns (map sql-identifier) (str/join ", ")) ")"
                   " VALUES (" (->> columns (map (constantly "?")) (str/join ", ")) ")")]
    (execute-query tx query values)))

(defn schema-exists? [tx schema]
  (let [query "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?"
        results (j/query tx [query schema])]
    (not (nil? (first results)))))

(defn create-schema [tx schema]
  (execute-query tx (str "CREATE SCHEMA " (sql-identifier schema))))

(defn table-exists? [tx schema table]
  (let [query "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?"
        results (j/query tx [query schema table])]
    (not (nil? (first results)))))

(defn create-table [tx schema table & fields]
  (execute-query tx (apply j/create-table-ddl (qualified-table schema table) fields)))

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
      (execute-query tx (str "CREATE INDEX " (quoted index-name)
                             " ON " (qualified-table schema table)
                             " USING " (name index-type) " (" quoted-columns ")"))
      (catch SQLException e
        (log/with-logs ['pdok.postgres :error :error] (j/print-sql-exception-chain e))
        (throw e)))))

(defn create-index [tx schema table & columns]
  (apply create-index* tx schema table :btree columns))

(defn kv->clause [[k v]]
  (if v
    (str (name k) " = '" (clojure.string/replace v #"'" "''") "'")
    (str (name k) " IS NULL")))

(defn map->where-clause
  ([clauses] (clojure.string/join " AND " (map kv->clause clauses)))
  ([clauses table] (clojure.string/join " AND " (map #(str table "." (kv->clause %)) clauses))))

(defn configure-auto-vacuum [tx schema table vacuum-scale-factor vacuum-threshold analyze-scale-factor]
  (try
    (do
      (execute-query tx (str "ALTER TABLE " (qualified-table schema table)
                             " SET (autovacuum_vacuum_scale_factor = " vacuum-scale-factor ")"))
      (execute-query tx (str "ALTER TABLE " (qualified-table schema table)
                             " SET (autovacuum_vacuum_threshold = " vacuum-threshold ")"))
      (execute-query tx (str "ALTER TABLE " (qualified-table schema table)
                             " SET (autovacuum_analyze_scale_factor = " analyze-scale-factor ")")))
    (catch SQLException e
      (log/with-logs ['pdok.postgres :error :error] (j/print-sql-exception-chain e))
      (throw e))))
