(ns pdok.featured.migrations.timeline.20151229-001-add-changelog
  (:require [pdok.postgres :as pg]
            [pdok.featured.dynamic-config :as dc]))

(defn- create-changelog [db]
  (pg/create-table db dc/*timeline-schema* dc/*timeline-changelog*
               [:id "serial" :primary :key]
               [:dataset "varchar(100)"]
               [:collection "varchar(255)"]
               [:feature_id "varchar(100)"]
               [:old_version "uuid"]
               [:new_version "uuid"]
               [:action "varchar(12)"]
               )
  (pg/create-index db dc/*timeline-schema* dc/*timeline-changelog* :new_version)
  (pg/create-index db dc/*timeline-schema* dc/*timeline-changelog* :dataset :collection))

(defn up [db]
  (create-changelog db))

(defn down [db])
