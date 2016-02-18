(ns pdok.featured.migrations.timeline.20151221-001-init
  (:require [pdok.postgres :as pg]
            [pdok.featured.dynamic-config :as dc]))

(defn- create-history-table [db]
  "Create table with default fields"
  (pg/create-table db dc/*timeline-schema* dc/*timeline-history-table*
               [:id "serial" :primary :key]
               [:collection "varchar(255)"]
               [:feature_id "varchar(100)"]
               [:version "uuid"]
               [:valid_from "timestamp without time zone"]
               [:valid_to "timestamp without time zone"]
               [:feature "text"]
               [:tiles "integer[]"])
  (pg/create-index db dc/*timeline-schema* dc/*timeline-history-table* :collection :feature_id)
  (pg/create-index db dc/*timeline-schema* dc/*timeline-history-table* :version))

(defn- create-current-table [db]
  (pg/create-table db dc/*timeline-schema* dc/*timeline-current-table*
               [:id "serial" :primary :key]
               [:collection "varchar(255)"]
               [:feature_id "varchar(100)"]
               [:version "uuid"]
               [:valid_from "timestamp without time zone"]
               [:valid_to "timestamp without time zone"]
               [:feature "text"]
               [:tiles "integer[]"])
  (pg/create-index db dc/*timeline-schema* dc/*timeline-current-table* :collection :feature_id)
  (pg/create-index db dc/*timeline-schema* dc/*timeline-current-table* :version))

(defn up [db]
  (create-history-table db)
  (create-current-table db))

(defn down [db])
