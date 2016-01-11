(ns pdok.featured.migrations.persistence.20151221-001-init
  (:require [pdok.postgres :as pg]
            [pdok.featured.dynamic-config :as dc]))

(defn up [db]
  (pg/create-table db dc/*persistence-schema* dc/*persistence-features*
                   [:id "bigserial" :primary :key]
                   [:dataset "varchar(100)"]
                   [:collection "varchar(100)"]
                   [:feature_id "varchar(50)"]
                   [:parent_collection "varchar(255)"]
                   [:parent_id "varchar(50)"]
                   [:parent_field "varchar(255)"])
  (pg/create-index db dc/*persistence-schema* dc/*persistence-features* :dataset :collection :feature_id)
  (pg/create-index db dc/*persistence-schema* dc/*persistence-features* :dataset :parent_collection :parent_id)
  (pg/create-table db dc/*persistence-schema* dc/*persistence-feature-stream*
                   [:id "bigserial" :primary :key]
                   [:version "uuid"]
                   [:action "varchar(12)"]
                   [:dataset "varchar(100)"]
                   [:collection "varchar(255)"]
                   [:feature_id "varchar(50)"]
                   [:validity "timestamp without time zone"]
                   [:geometry "text"]
                   [:attributes "text"])
  (pg/create-index db dc/*persistence-schema* dc/*persistence-feature-stream* :dataset :collection :feature_id))

(defn down [db])
