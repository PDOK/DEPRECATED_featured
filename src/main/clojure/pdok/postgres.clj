(ns pdok.postgres
  (:require [clojure.java.jdbc :as j]
            [clj-time [coerce :as tc]]))

(extend-protocol j/ISQLValue
  org.postgis.Geometry
  (sql-value [v] v))

(extend-protocol j/ISQLParameter
  org.postgis.Geometry
  (set-parameter [v ^java.sql.PreparedStatement s ^long i]
    (.setObject s i v java.sql.Types/OTHER)))

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

(defn table-columns [db schema table]
  "Get table columns"
  (j/with-db-connection [c db]
    (let [results
          (j/query c ["SELECT column_name
FROM information_schema.columns
  WHERE table_schema = ?
  AND table_name   = ?" schema table])]
      results)))

(defn clj-to-db-type [type]
  "Returns type with transformation method"
  (condp = type
    org.joda.time.DateTime ["timestamp without time zone" #(tc/to-timestamp %)]
    ["text" #(str %)]))

(defn convert-clj-to-pg [value]
  (let [[_ conversion-fn] (clj-to-db-type (type value))]
    (conversion-fn value)))

(defn get-clj-pg-type [value]
  (let [[pg-type _] (clj-to-db-type (type value))]
    pg-type))

(defn add-column [db schema collection column-name column-type]
  (let [template "ALTER TABLE %s.%s ADD %s %s NULL;"
        [clj-type _] (clj-to-db-type column-type)
        cmd (format template (quoted schema) (quoted collection) (quoted column-name) clj-type)]
    (j/db-do-commands db cmd)))
