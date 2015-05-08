(ns pdok.postgres
  (:require [clojure.java.jdbc :as j]
            [clj-time [coerce :as tc]]
            [cognitect.transit :as transit])
  (:import [com.vividsolutions.jts.geom Geometry]
           [com.vividsolutions.jts.io WKTWriter]
           [java.io ByteArrayOutputStream]))

(def wkt-writer (WKTWriter.))

(def joda-time-writer
  (transit/write-handler
   (constantly "m")
   (fn [v] (-> v tc/to-date .getTime))
   (fn [v] (-> v tc/to-date .getTime .toString))))

(def transit-handlers (atom {}))

(defn register-transit-handler [type handler]
  (swap! transit-handlers assoc type handler))

(register-transit-handler org.joda.time.DateTime joda-time-writer)

(defn to-json [obj]
  (let [out (ByteArrayOutputStream. 1024)
        writer (transit/writer out :json
                       {:handlers @transit-handlers})]
    (transit/write writer obj)
    (.toString out))
  )

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

(defn add-geo-constraints [db schema table geometry-column]
  (let [ndims (db-constraint schema table geometry-column "enforce_dims_geom" "public.st_ndims" "= 2")
        srid (db-constraint schema table geometry-column "enforce_srid_geom" "public.st_srid" "= 28992")
        geotype (db-constraint schema table geometry-column "enforce_geotype_geom" "public.geometrytype" "IN ('POINT'::text, 'MULTIPOINT'::text, 'LINESTRING'::text, 'MULTILINESTRING'::text, 'POLYGON'::text, 'MULTIPOLYGON'::text, 'CIRCULARSTRING'::text, 'COMPOUNDCURVE'::text, 'MULTICURVE'::text, 'CURVEPOLYGON'::text, 'MULTISURFACE'::text, 'GEOMETRY'::text, 'GEOMETRYCOLLECTION'::text, 'POINTM'::text, 'MULTIPOINTM'::text, 'LINESTRINGM'::text, 'MULTILINESTRINGM'::text, 'POLYGONM'::text, 'MULTIPOLYGONM'::text, 'CIRCULARSTRINGM'::text, 'COMPOUNDCURVEM'::text, 'MULTICURVEM'::text, 'CURVEPOLYGONM'::text, 'MULTISURFACEM'::text, 'GEOMETRYCOLLECTIONM'::text)")
        constraints (vector ndims srid geotype)]
  (try 
    (doseq [c constraints]
      (j/db-do-commands db c))
    (catch java.sql.SQLException e (j/print-sql-exception-chain e)))))

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
