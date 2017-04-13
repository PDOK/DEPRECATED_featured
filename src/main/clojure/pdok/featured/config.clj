(ns pdok.featured.config
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            clojure.stacktrace
            [pdok.featured
             [projectors :as proj]
             [persistence :as pers]
             [geoserver :as gs]
             [timeline :as timeline]]
            [environ.core :as environ]
             [clojure.core.async :refer [<!!]]))

(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread throwable]
     (log/error throwable "Stacktrace:"
                (print-str (clojure.stacktrace/print-stack-trace throwable))))))

(defn- keywordize [s]
  (-> (str/lower-case s)
      (str/replace "_" "-")
      (str/replace "." "-")
      (keyword)))

(defn load-props [resource-file]
  (with-open [^java.io.Reader reader (io/reader (io/resource resource-file))]
    (let [props (java.util.Properties.)
          _ (.load props reader)]
      (into {} (for [[k v] props
                     ;; no mustaches, for local use
                     :when (not (re-find #"^\{\{.*\}\}$" v))]
                 [(keywordize k) v])))))

(defonce env
  (merge environ/env
         (load-props "plp.properties")))

(def processor-db {:subprotocol "postgresql"
                   :subname (or (env :processor-database-url) "//localhost:5432/pdok")
                   :user (or (env :processor-database-user) "postgres")
                   :password (or (env :processor-database-password) "postgres")
                   :transaction? true})

(def data-db {:subprotocol "postgresql"
              :subname (or (env :data-database-url) "//localhost:5432/pdok")
              :user (or (env :data-database-user) "postgres")
              :password (or (env :data-database-password) "postgres")
              :transaction? true})

(def extracts-db {:subprotocol  "postgresql"
                  :subname      (or (env :extracts-database-url) "//localhost:5432/pdok")
                  :user         (or (env :extracts-database-user) "postgres")
                  :password     (or (env :extracts-database-password) "postgres")
                  :transaction? true
                  :schema       (or (env :extracts-schema) "extractmanagement")})

(defn persistence []
  (pers/make-cached-jdbc-processor-persistence
                    {:db-config processor-db :batch-size 10000}))

(def store-dir
  (let [fqdn (or (env :fully-qualified-domain-name) "localhost")
        s (System/getProperty "file.separator")
        path (io/file (str (System/getProperty "java.io.tmpdir") s "featured" s fqdn s ))]
    path))

(defn filestore
  ([]
   (let [path store-dir]
     (filestore path)))
  ([store-dir]
   (let [path (io/file store-dir)]
     (when-not (.exists path) (.mkdirs path))
     {:store-dir path})))

(defn timeline
  ([]
   (timeline (persistence)))
  ([persistence]
   (timeline persistence (filestore (str (System/getProperty "java.io.tmpdir") "/featured"))))
  ([persistence filestore]
   (timeline/create-chunked {:chunk-size  (read-string (or (env :processor-batch-size) "10000"))
                             :db-config   processor-db
                             :persistence persistence}
                            filestore)))

(defn timeline-for-dataset [dataset]
  (let [tl (timeline)]
    (proj/init tl dataset #{})))

(def projections
  {:RD {:proj-fn pdok.featured.feature/as-rd
        :ndims 2
        :srid 28992}
   :ETRS89 {:proj-fn pdok.featured.feature/as-etrs89
            :ndims 2
            :srid 4258}})

(def default-projection
  {:proj-fn identity
   :ndims 2
   :srid 28992})

(defn projectors [persistence & {:keys [projection no-visualization]}]
   (let [{:keys [proj-fn ndims srid]} (get projections (keyword projection))]
     [(gs/geoserver-projector {:db-config data-db
                               :proj-fn proj-fn
                               :ndims ndims
                               :srid srid
                               :no-visualization no-visualization
                               :import-nil-geometry? true})]))

(defn create-workers [factory-f]
  (let [n-workers (read-string (or (env :n-workers) "2"))]
    (dorun (for [i (range 0 n-workers)]
             (factory-f i)))))

(def cleanup-threshold
  (read-string (or (env :cleanup-threshold) "5")))

(defn create-url [path]
  (let [fully-qualified-domain-name (or (env :fully-qualified-domain-name) "localhost")
        port (or (env :port) "3000")
        context-root (or (env :context-root) nil)]
    (str "http://" fully-qualified-domain-name ":" port "/" context-root (when context-root "/") path)))
