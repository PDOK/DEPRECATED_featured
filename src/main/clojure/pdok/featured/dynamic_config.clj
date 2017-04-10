(ns pdok.featured.dynamic-config)

(def ^:dynamic *persistence-schema-prefix* :featured)
(def ^:dynamic *persistence-schema* "for init only")
(def ^:dynamic *persistence-features* :feature)
(def ^:dynamic *persistence-feature-stream* :feature_stream)
(def ^:dynamic *persistence-collections* :collections)

(def ^:dynamic *timeline-schema-prefix* :featured)
(def ^:dynamic *timeline-schema* "for init only")
(def ^:dynamic *timeline-table* :timeline)
