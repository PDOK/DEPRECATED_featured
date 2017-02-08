(ns pdok.postgres
  (:require [clojure.java.jdbc :as j]
            [clj-time [coerce :as tc]]
            [clojure.tools.logging :as log]
            [pdok.transit :as transit]
            [pdok.featured.feature :as f])
  (:import [com.vividsolutions.jts.io WKTWriter]
           [java.util Calendar TimeZone]
           [org.joda.time DateTimeZone LocalDate LocalDateTime]
           (pdok.featured NilAttribute)
           (java.sql Types)))

(defn dbspec->url [{:keys [subprotocol subname user password]}]
  (str "jdbc:" subprotocol ":" subname "?user=" user "&password=" password))

(def wkt-writer (WKTWriter.))

(def utcCal (Calendar/getInstance (TimeZone/getTimeZone "UTC")))
(def nlZone (DateTimeZone/getDefault)) ;; used for reading datetimes, because postgres returns Z values

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
  (result-set-read-column [v _ _] (LocalDate. ^java.sql.Date v ^DateTimeZone nlZone))
  java.sql.Timestamp
  (result-set-read-column [v _ _] (LocalDateTime. ^java.sql.Timestamp v  ^DateTimeZone nlZone))
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

(defn schema-exists? [db schema]
  (j/with-db-connection [c db]
    (let [results
          (j/query c ["SELECT 1 FROM information_schema.schemata WHERE schema_name = ?" schema])]
      (not (nil? (first results)))))
  )

(defn create-schema [db schema]
  "Create schema"
  (j/db-do-commands db (str "CREATE SCHEMA " (-> schema name quoted))))


(defn table-exists? [db schema table]
  (j/with-db-connection [c db]
    (let [results
          (j/query c ["SELECT 1 FROM information_schema.tables WHERE  table_schema = ? AND table_name = ?"
                      schema table])]
      (not (nil? (first results)))))
  )

(defn create-table [db schema table & fields]
  (j/db-do-commands db
                    (apply j/create-table-ddl (str (-> schema name quoted) "." (-> table name quoted)) fields)))

(defn- remove-underscores [name]
  (if (= (first name) \_)
    (recur (next name))
    (apply str name)))

(defn- create-index* [db schema table index-type & columns]
  (let [column-names (map name columns)
        quoted-columns (clojure.string/join "," (map quoted column-names))
        index-name (str (name table) "_" (clojure.string/join "_" (map remove-underscores column-names)) (or (index-type {:gist "_sidx"}) "_idx"))]
    (try
      (j/db-do-commands db (str "CREATE INDEX " (quoted index-name)
                                " ON "  (-> schema name quoted) "." (-> table name quoted)
                                " USING " (name index-type) " (" quoted-columns ")" ))
      (catch java.sql.SQLException e
        (log/with-logs ['pdok.postgres :error :error] (j/print-sql-exception-chain e))
        (throw e))))
  )

(defn create-index [db schema table & columns]
  (apply create-index* db schema table :btree columns))

(defn create-geo-index [db schema table & columns]
  (apply create-index* db schema table :gist columns))

(defn table-columns [db schema table]
  "Get table columns"
  (j/with-db-connection [c db]
    (let [results
          (j/query c ["SELECT column_name
FROM information_schema.columns
  WHERE table_schema = ?
  AND table_name   = ?" schema table])]
      results)))

(defn- execute-checked-sql [db cmd]
  (try
    (j/db-do-commands db cmd)
    (catch java.sql.SQLException e
      (log/with-logs ['pdok.postgres :error :error] (j/print-sql-exception-chain e))
      (throw e))))

(defn create-column [db schema table column-name type ndims srid]
  (let [cmd (condp = type
              geometry-type (str "SELECT public.AddGeometryColumn ('" schema "', '" table "', '" (-> column-name name) "', " srid ", 'GEOMETRY', " ndims ")")
              (format "ALTER TABLE %s.%s ADD %s %s NULL;" (-> schema name quoted) (-> table name quoted) (-> column-name name quoted) type))]
    (execute-checked-sql db cmd)))

(defn kv->clause [[k v]]
  (if v
    (str (name k) " = '" (clojure.string/replace v #"'" "''") "'")
    (str (name k) " is null")))

(defn map->where-clause
  ([clauses] (clojure.string/join " AND " (map kv->clause clauses)))
  ([clauses table] (clojure.string/join " AND " (map #(str table "." (kv->clause %)) clauses))))

(defn configure-auto-vacuum [db schema table vacuum-scale-factor vacuum-threshold analyze-scale-factor]
  (try
    (j/db-do-commands db
                      (str "ALTER TABLE " (-> schema name quoted) "." (-> table name quoted)
                           " SET (autovacuum_vacuum_scale_factor = " vacuum-scale-factor ")")
                      (str "ALTER TABLE " (-> schema name quoted) "." (-> table name quoted)
                           " SET (autovacuum_vacuum_threshold = " vacuum-threshold ")")
                      (str "ALTER TABLE " (-> schema name quoted) "." (-> table name quoted)
                           " SET (autovacuum_analyze_scale_factor = " analyze-scale-factor ")"))
    (catch java.sql.SQLException e
      (log/with-logs ['pdok.postgres :error :error] (j/print-sql-exception-chain e))
      (throw e))))
