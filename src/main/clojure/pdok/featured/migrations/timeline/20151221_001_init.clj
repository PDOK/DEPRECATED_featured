(ns pdok.featured.migrations.timeline.20151221-001-init
  (:require [pdok.postgres :as pg]))

(def ^:dynamic *timeline-schema* "featured")
(def ^:dynamic *history-table* "timeline")
(def ^:dynamic *current-table* "timeline_current")

(defn- create-history-table [db]
  "Create table with default fields"
  (pg/create-table db *timeline-schema* *history-table*
               [:id "serial" :primary :key]
               [:dataset "varchar(100)"]
               [:collection "varchar(255)"]
               [:feature_id "varchar(100)"]
               [:version "uuid"]
               [:valid_from "timestamp without time zone"]
               [:valid_to "timestamp without time zone"]
               [:feature "text"]
               [:tiles "integer[]"])
  (pg/create-index db *timeline-schema* *history-table* :dataset :collection :feature_id)
  (pg/create-index db *timeline-schema* *history-table* :version))

(defn- create-current-table [db]
  (pg/create-table db *timeline-schema* *current-table*
               [:id "serial" :primary :key]
               [:dataset "varchar(100)"]
               [:collection "varchar(255)"]
               [:feature_id "varchar(100)"]
               [:version "uuid"]
               [:valid_from "timestamp without time zone"]
               [:valid_to "timestamp without time zone"]
               [:feature "text"]
               [:tiles "integer[]"])
  (pg/create-index db *timeline-schema* *current-table* :dataset :collection :feature_id)
  (pg/create-index db *timeline-schema* *current-table* :version))

(defn up [db]
  (create-history-table db)
  (create-current-table db))

(defn down [db])
