(ns pdok.featured.migrations.timeline.20151229-001-add-delta-tables
  (:require [pdok.postgres :as pg]
            [pdok.featured.dynamic-config :as dc]))

(defn- create-history-delta-table [db]
  (pg/create-table db dc/*timeline-schema* dc/*timeline-history-delta-table*
               [:id "serial" :primary :key]
               [:dataset "varchar(100)"]
               [:collection "varchar(255)"]
               [:version "uuid"]
               [:action "varchar(1)"] ; D(elete) or I(insert)
            ))

(defn- create-current-delta-table [db]
  (pg/create-table db dc/*timeline-schema* dc/*timeline-current-delta-table*
               [:id "serial" :primary :key]
               [:dataset "varchar(100)"]
               [:collection "varchar(255)"]
               [:version "uuid"]
               [:action "varchar(1)"] ; D(elete) or I(insert)
            ))


(defn up [db]
  (create-history-delta-table db)
  (create-current-delta-table db))

(defn down [db])
