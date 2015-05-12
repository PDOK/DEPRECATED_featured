(ns pdok.postgres
  (:require [clojure.java.jdbc :as j]
            [clj-time [coerce :as tc]]
            [cognitect.transit :as transit])
  (:import [com.vividsolutions.jts.geom Geometry]
           [com.vividsolutions.jts.io WKTWriter]
           [java.io ByteArrayOutputStream ByteArrayInputStream]))

(def wkt-writer (WKTWriter.))

(def joda-time-writer
  (transit/write-handler
   (constantly "m")
   (fn [v] (-> v tc/to-date .getTime))
   (fn [v] (-> v tc/to-date .getTime .toString))))

(def joda-time-reader
  (transit/read-handler #(tc/from-long (Long/parseLong %))))

(def transit-write-handlers (atom {}))
(def transit-read-handlers (atom {}))

(defn register-transit-write-handler [type handler]
  (swap! transit-write-handlers assoc type handler))

(defn register-transit-read-handler [prefix handler]
  (swap! transit-read-handlers assoc prefix handler))

(register-transit-write-handler org.joda.time.DateTime joda-time-writer)
(register-transit-read-handler "m" joda-time-reader)

(defn to-json [obj]
  (let [out (ByteArrayOutputStream. 1024)
        writer (transit/writer out :json
                       {:handlers @transit-write-handlers})]
    (transit/write writer obj)
    (.toString out))
  )

(defn from-json [str]
  (let [in (ByteArrayInputStream. (.getBytes str))
        reader (transit/reader in :json
                               {:handlers @transit-read-handlers})]
    (transit/read reader)))

(extend-protocol j/ISQLValue
  org.joda.time.DateTime
  (sql-value [v] (tc/to-timestamp v))
  com.vividsolutions.jts.geom.Geometry
  (sql-value [v] (str "SRID=28992;" (.write wkt-writer v)))
  clojure.lang.Keyword
  (sql-value [v] (name v))
  clojure.lang.IPersistentMap
  (sql-value [v] (to-json v)))

(extend-protocol j/ISQLParameter
  java.util.UUID
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) java.sql.Types/OTHER))
  com.vividsolutions.jts.geom.Geometry
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) java.sql.Types/OTHER))
  clojure.lang.IPersistentMap
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) java.sql.Types/VARCHAR)))

(defn clj-to-pg-type [clj-type]
  (condp = clj-type
    nil "text"
    clojure.lang.Keyword "text"
    clojure.lang.IPersistentMap "text"
    org.joda.time.DateTime "timestamp without time zone"
    java.lang.Integer "integer"
    java.lang.Double "double precision"
    java.lang.Boolean "boolean"
    java.util.UUID "uuid"
    "text"))

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

(defn create-index [db schema table & columns]
  (let [column-names (map name columns)
        index-name (str (name table) (clojure.string/join "" column-names) "_idx")
        quoted-columns (clojure.string/join "," (map quoted column-names))]
    (try
      (j/db-do-commands db (str "CREATE INDEX " (quoted index-name)
                                " ON "  (-> schema name quoted) "." (-> table name quoted)
                                " USING btree (" quoted-columns ")" ))
      (catch java.sql.SQLException e (j/print-sql-exception-chain e)))))

(defn- db-constraint [schema table geo-column constraint-name constraint constraint-pred]
  (let [schema-name-quoted (-> schema name quoted)
        table-name-quoted (-> table name quoted)
        geo-column-quoted (-> geo-column name quoted)]
  (str "ALTER TABLE " schema-name-quoted "." table-name-quoted " ADD CONSTRAINT " constraint-name " CHECK (" geo-column-quoted " IS NULL OR " constraint "(" geo-column-quoted ")" constraint-pred ")")))

(def geo-constraints
  {:_geometry_point "'POINT'::text, 'MULTIPOINT'::text, 'POINTM'::text, 'MULTIPOINTM'::text" 
   :_geometry_line "'LINESTRING'::text, 'MULTILINESTRING'::text, 'LINESTRINGM'::text, 'MULTILINESTRINGM'::text" 
   :_geometry_polygon "'POLYGON'::text, 'MULTIPOLYGON'::text, 'CIRCULARSTRING'::text, 'COMPOUNDCURVE'::text, 'MULTICURVE'::text, 'CURVEPOLYGON'::text, 'MULTISURFACE'::text, 'GEOMETRY'::text, 'GEOMETRYCOLLECTION'::text, 'POLYGONM'::text, 'MULTIPOLYGONM'::text, 'CIRCULARSTRINGM'::text, 'COMPOUNDCURVEM'::text, 'MULTICURVEM'::text, 'CURVEPOLYGONM'::text, 'MULTISURFACEM'::text, 'GEOMETRYCOLLECTIONM'::text"})

(defn add-geo-constraints [db schema table geometry-column]
  (let [column-name (-> geometry-column name)
        ndims (db-constraint schema table geometry-column (str "enforce_dims_" column-name) "public.st_ndims" "= 2")
        srid (db-constraint schema table geometry-column (str "enforce_srid_" column-name) "public.st_srid" "= 28992")
        geotype (db-constraint schema table geometry-column (str "enforce_geotype_" column-name) "public.geometrytype" (str "IN (" (-> column-name keyword geo-constraints) ")" ) )
        constraints (vector ndims srid geotype)]
  (try 
    (doseq [c constraints]
      (j/db-do-commands db c))
    (catch java.sql.SQLException e (j/print-sql-exception-chain e)))))

(defn populate-geometry-columns [db schema table]
  (try 
    (j/query db [(str "SELECT public.populate_geometry_columns (( SELECT '" (-> schema name quoted) "."  (-> table name quoted) "'::regclass::oid ))")])
    (catch java.sql.SQLException e (j/print-sql-exception-chain e))))

(defn table-columns [db schema table]
  "Get table columns"
  (j/with-db-connection [c db]
    (let [results
          (j/query c ["SELECT column_name
FROM information_schema.columns
  WHERE table_schema = ?
  AND table_name   = ?" schema table])]
      results)))

(defn add-column [db schema collection column-name column-type]
  (let [template "ALTER TABLE %s.%s ADD %s %s NULL;"
        clj-type (clj-to-pg-type column-type)
        cmd (format template (-> schema name quoted) (-> collection name quoted) (-> column-name name quoted) clj-type)]
    (j/db-do-commands db cmd)))
