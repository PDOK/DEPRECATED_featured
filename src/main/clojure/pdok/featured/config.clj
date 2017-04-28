(ns pdok.featured.config
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            clojure.stacktrace
            [pdok.featured
             [persistence :as pers]
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
   (timeline (filestore (str (System/getProperty "java.io.tmpdir") "/featured"))))
  ([filestore]
   (timeline/create-chunked {:chunk-size (read-string (or (env :processor-batch-size) "10000"))} filestore)))

(defn create-workers [factory-f]
  (let [n-workers (read-string (or (env :n-workers) "2"))]
    (dorun (for [i (range 0 n-workers)]
             (factory-f i)))))

(def cleanup-threshold
  (read-string (or (env :cleanup-threshold) "5")))

(defn create-url [path]
  (let [fully-qualified-domain-name (or (env :fully-qualified-domain-name) "localhost")
        port (or (env :port) "8000")
        context-root (or (env :context-root) nil)]
    (str "http://" fully-qualified-domain-name ":" port "/" context-root (when context-root "/") path)))
