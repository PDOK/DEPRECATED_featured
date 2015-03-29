(ns pdok.postgres
  (:require [clojure.java.jdbc :as j]
            [clj-time [coerce :as tc]])
  (:import [com.vividsolutions.jts.geom Geometry]
           [com.vividsolutions.jts.io WKTWriter]))

(defprotocol IPostgresType
  (clj-to-pg-type [v]))

(def wkt-writer (WKTWriter.))

(extend-protocol j/ISQLValue
  org.joda.time.DateTime
  (sql-value [v] (tc/to-timestamp v))
  com.vividsolutions.jts.geom.Geometry
  (sql-value [v] (.write wkt-writer v)))

(extend-protocol j/ISQLParameter
  com.vividsolutions.jts.geom.Geometry
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (.setObject s i (j/sql-value v) java.sql.Types/OTHER)))

(extend-protocol IPostgresType
  Object
  (clj-to-pg-type [_] "text")
  nil
  (clj-to-pg-type [_] nil)
  org.joda.time.DateTime
  (clj-to-pg-type  [_] "timestamp without time zone"))

(def quoted (j/quoted \"))

(defn schema-exists? [db schema]
  (j/with-db-connection [c db]
    (let [results
          (j/query c ["SELECT 1 FROM information_schema.schemata WHERE schema_name = ?" schema])]
      (not (nil? (first results)))))
  )

(defn create-schema [db schema]
  "Create schema"
  (j/db-do-commands db (str "CREATE SCHEMA " (quoted schema))))


(defn table-exists? [db schema table]
  (j/with-db-connection [c db]
    (let [results
          (j/query c ["SELECT 1 FROM information_schema.tables WHERE  table_schema = ? AND table_name = ?"
                      schema table])]
      (not (nil? (first results)))))
  )

(defn create-table [db schema table & fields]
  (j/db-do-commands db
                    (apply j/create-table-ddl (str schema "." table) fields)))

(defn create-index [db schema table column]
  (j/db-do-commands db (str "CREATE INDEX " (quoted (str column "_idx"))
                            " ON "  (quoted schema) "." (quoted table)
                            "  USING btree (" (quoted column) ")" )))

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
        cmd (format template (quoted schema) (quoted collection) (quoted column-name) clj-type)]
    (j/db-do-commands db cmd)))
