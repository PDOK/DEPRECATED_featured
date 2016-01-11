(ns pdok.featured.dynamic-config)

(def ^:dynamic *persistence-schema* :featured)
(def ^:dynamic *persistence-features* :feature)
(def ^:dynamic *persistence-feature-stream* :feature_stream)
(def ^:dynamic *persistence-migrations* :persistence_migrations)

(def ^:dynamic *timeline-schema* "featured")
(def ^:dynamic *timeline-current-table* "timeline_current")
(def ^:dynamic *timeline-current-delta-table* "timeline_current_delta")
(def ^:dynamic *timeline-history-table* "timeline")
(def ^:dynamic *timeline-history-delta-table* "timeline_delta")
(def ^:dynamic *timeline-migrations* "timeline_migrations")
