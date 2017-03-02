(ns pdok.featured.timeline
  (:refer-clojure :exclude [merge])
  (:require [pdok.cache :refer :all]
            [pdok.featured.dynamic-config :as dc]
            [pdok.postgres :as pg]
            [pdok.util :refer [with-bench checked] :as util]
            [pdok.featured.projectors :as proj]
            [pdok.featured.persistence :as pers]
            [pdok.transit :as transit]
            [pdok.filestore :as fs]
            [clojure.java.jdbc :as j]
            [clojure.core.cache :as cache]
            [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [clojure.core.async :refer [>!! close! chan]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk])
  (:import [clojure.lang PersistentQueue]
           (java.io OutputStream)
           (java.sql SQLException)
           (java.util.zip ZipEntry ZipOutputStream)))

(declare create)

(defn schema [dataset]
  (str (name dc/*timeline-schema-prefix*) "_" dataset))

(defn- qualified-timeline [dataset collection]
  (str (pg/quoted (schema dataset)) "." (pg/quoted (str (name dc/*timeline-table*) "_" collection))))

(defn- init [db dataset]
  (when-not (pg/schema-exists? db (schema dataset))
    (checked (pg/create-schema db (schema dataset))
             (pg/schema-exists? db (schema dataset)))))

(defn- init-collection [db dataset collection]
  (let [table-name (str (name dc/*timeline-table*) "_" collection)]
    (with-bindings {#'dc/*timeline-schema* (schema dataset)}
      (when-not (pg/table-exists? db dc/*timeline-schema* table-name)
        (checked (do (pg/create-table db dc/*timeline-schema* table-name
                                      [:id "serial" :primary :key]
                                      [:feature_id "varchar(100)"]
                                      [:version "uuid"]
                                      [:valid_from "timestamp without time zone"]
                                      [:valid_to "timestamp without time zone"]
                                      [:feature "text"]
                                      [:tiles "integer[]"])
                     (pg/create-index db dc/*timeline-schema* table-name :feature_id)
                     (pg/create-index db dc/*timeline-schema* table-name :version))
                 (pg/table-exists? db dc/*timeline-schema* table-name))))))

(defn- execute-query [timeline query]
  (try (j/with-db-connection [c (:db timeline)]
         (let [results (j/query c [query] :as-arrays? true)
               results (map (fn [[f]] (transit/from-json f)) (drop 1 results))]
           results))
       (catch SQLException e
         (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
         (throw e))))

(defn current
  ([{:keys [dataset] :as timeline} collection]
   (let [query (str "SELECT feature FROM " (qualified-timeline dataset collection) " "
                    "WHERE valid_to is null")]
     (execute-query timeline query))))

(defn- init-root
  ([feature]
   (init-root (:collection feature) (:id feature)))
  ([collection id]
   {:_collection collection :_id id}))

(defn keywordize-keys-and-remove-nils
  "Recursively transforms all map keys from strings to keywords and removes all pairs with nil values."
  [m]
  (let [f (fn [[k v]] (when (not (nil? v)) (if (string? k) [(keyword k) v] [k v])))]
    (walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) m)))

(defn- mustafy [feature]
  (reduce
    (fn [acc [k v]] (if v
                      (assoc acc (keyword k) (keywordize-keys-and-remove-nils v))
                      acc))
    (init-root feature)
    (:attributes feature)))

(defn merge* [target action feature]
  (condp = action
    :close target
    (clojure.core/merge target feature)))

(defn merge
  ([target feature]
   (let [mustafied (mustafy feature)
         merged (merge* target (:action feature) mustafied)
         merged (assoc merged :_version (:version feature))
         merged (update-in merged [:_all_versions] (fnil conj '()) (:version feature))
         merged (update merged :_tiles #((fnil clojure.set/union #{}) % (:tiles feature)))]
     merged)))

(defn- sync-valid-from [acc feature]
  (assoc acc :_valid_from  (:validity feature)))

(defn- sync-valid-to [acc feature]
  (let [changed (assoc acc :_valid_to (:validity feature))]
    changed))

(defn- reset-valid-to [acc]
  (assoc acc :_valid_to nil))

(defn- new-current-sql [dataset collection]
  (str "INSERT INTO " (qualified-timeline dataset collection)
       " (feature_id, version, valid_from, valid_to, feature, tiles)
VALUES (?, ?, ?, ?, ?, ?)"))

;;(def feature-info (juxt :_collection :_id #(.toString (:_valid_from %)) #(when (:_valid_to %1) (.toString (:_valid_to %1))) :_version))

(defn- new-current
  ([{:keys [db dataset]} features]
    ;(println "NEW-CURRENT" (map feature-info features))
   (try
     (let [per-collection (group-by :_collection features)]
       (doseq [[collection collection-features] per-collection]
         (let [transform-fn (juxt :_id :_version
                                  :_valid_from :_valid_to transit/to-json :_tiles)
               records (map transform-fn collection-features)]
           (j/execute! db (cons (new-current-sql dataset collection) records) :multi? true :transaction? (:transaction? db)))))
     (catch SQLException e
       (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
       (throw e)))))

(defn- load-current-feature-cache-sql [dataset collection n]
  (str "SELECT DISTINCT ON (feature_id)
    feature_id, feature
    FROM " (qualified-timeline dataset collection)
    " WHERE feature_id in ("
       (clojure.string/join "," (repeat n "?")) ")
      ORDER BY feature_id ASC, version DESC, valid_from DESC"))

(defn- load-current-feature-cache [db dataset collection ids]
  (try (j/with-db-connection [c db]
         (let [results
               (j/query c (apply vector (load-current-feature-cache-sql dataset collection (count ids))
                                  ids) :as-arrays? true)
               for-cache
               (map (fn [[fid f]] [[collection fid] (transit/from-json f)] ) (drop 1 results))]
           for-cache))
       (catch SQLException e
         (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
         (throw e))))

(defn- delete-version-sql [dataset collection]
  (str "DELETE FROM " (qualified-timeline dataset collection)
       " WHERE version = ?"))

(defn- delete-version-with-valid-from-sql [dataset collection]
  (str "DELETE FROM " (qualified-timeline dataset collection)
       " WHERE version = ? AND valid_from = ? AND valid_to is null"))

(defn- delete-version-with-valid-from-and-valid-to-sql [dataset collection]
  (str "DELETE FROM " (qualified-timeline dataset collection)
       " WHERE version = ? AND valid_from = ? AND valid_to = ?"))

(defn- delete-feature-with-version [{:keys [db dataset]} records]
  "([collection version] ... )"
  (let [per-collection (group-by first records)]
    (doseq [[collection collection-records] per-collection]
      (let [versions (map #(vector (nth % 1))  collection-records)]
        (when (seq versions)
          (try
            (j/execute! db (cons (delete-version-sql dataset collection) versions) :multi? true :transaction? (:transaction? db))
            (catch SQLException e
              (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
              (throw e))))))))

(defn- delete-feature-with-version-and-validity [{:keys [db dataset]} records]
  "([collection version valid-from valid-to] ... )"
  (let [per-collection (group-by first records)]
    (doseq [[collection collection-records] per-collection]
      (let [{open true closed false} (group-by (fn [[_ _ valid-to]] (nil? valid-to)) (map #(drop 1 %) collection-records))]
        (when (seq open)
          (try
            (j/execute! db (cons (delete-version-with-valid-from-sql dataset collection) (map #(take 2 %) open)) :multi? true :transaction? (:transaction? db))
            (catch SQLException e
              (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
              (throw e))))
        (when (seq closed)
          (try
            (j/execute! db (cons (delete-version-with-valid-from-and-valid-to-sql dataset collection) closed) :multi? true :transaction? (:transaction? db))
            (catch SQLException e
              (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
              (throw e))))))))

(def o ^{:private true} (Object.))
(defn unique-epoch []
  (locking o
    (Thread/sleep 1) ;; ugh, ugly
    (System/currentTimeMillis)))

(defn ^{:dynamic true} *changelog-name* [dataset collection]
  (str (unique-epoch) "-" dataset "-" collection ".changelog"))

(defn changelog-new-entry [new-feature]
  "[collection] action id version json"
  [(:_collection new-feature)
   (transit/to-json {:action "new"
                     :collection (:_collection new-feature)
                     :id (:_id new-feature)
                     :version (:_version new-feature)
                     :tiles (:_tiles new-feature)
                     :valid-from (:_valid_from new-feature)
                     :attributes (apply dissoc new-feature
                                        [:_collection :_id :_version :_all_versions :_tiles :_valid_from :_valid_to])
                     })])

(defn changelog-change-entry [current-version new-feature]
  "[collection] action id old-version version json"
  [(:_collection new-feature)
   (transit/to-json {:action "change"
                     :collection (:_collection new-feature)
                     :id (:_id new-feature)
                     :previous-version current-version
                     :version (:_version new-feature)
                     :tiles (:_tiles new-feature)
                     :valid-from (:_valid_from new-feature)
                     :attributes (apply dissoc new-feature
                                        [:_collection :_id :_version :_all_versions :_tiles :_valid_from :_valid_to])
                     })])

(defn changelog-close-entry [old-version closed-feature]
  "[collection] action id old-version version json"
  [(:_collection closed-feature)
   (transit/to-json {:action "close"
                     :collection (:_collection closed-feature)
                     :id (:_id closed-feature)
                     :previous-version old-version
                     :version (:_version closed-feature)
                     :tiles (:_tiles closed-feature)
                     :valid-from (:_valid_from closed-feature)
                     :valid-to (:_valid_to closed-feature)
                     :attributes (apply dissoc closed-feature
                                        [:_collection :_id :_version :_all_versions :_tiles :_valid_from :_valid_to])
                     })])

(defn changelog-delete-entry [collection id version]
  "[collection] action id old-version"
  [collection
   (transit/to-json {:action "delete"
                     :collection collection
                     :id id
                     :previous-version version
                     })])

(defn- write-changelog-entries [^OutputStream stream, collection, entries]
  "Write the contents of one changelog file to an arbitrary output stream"
  (let [write-fn (fn [^String str] (.write stream (.getBytes str)))]
    (write-fn "pdok-featured-changelog-v2\n")
    (write-fn (str (transit/to-json {:collection collection}) "\n"))
    (doseq [entry entries]
      ; pop collection from front
      (write-fn (str (second entry) "\n")))))

(defn- append-to-changelog [{:keys [dataset changelogs filestore]} log-entries]
  "Write a batch of changelog entries to separate zip files per collection"
  (let [per-collection (group-by first log-entries)]
    (doseq [[collection entries] per-collection]
      (let [^String uncompressed-filename (*changelog-name* dataset collection)
            changelog (str uncompressed-filename ".zip")
            compressed-file (fs/create-target-file filestore changelog)
            _ (swap! changelogs conj changelog)]
        (with-open [output-stream (io/output-stream compressed-file)
                    zip (ZipOutputStream. output-stream)]
          (.putNextEntry zip (ZipEntry. uncompressed-filename))
          (write-changelog-entries zip collection entries)
          (.closeEntry zip))))))

(defn- feature-key [feature]
  [(:collection feature) (:id feature)])

(defn- make-flush-all [new-batch delete-batch delete-for-update-batch changelog-batch]
  "Used for flushing all batches, so update current is always performed after new current"
  (fn [timeline]
    (fn []
      (flush-batch new-batch (partial new-current timeline))
      (flush-batch delete-for-update-batch (partial delete-feature-with-version-and-validity timeline))
      (flush-batch delete-batch (partial delete-feature-with-version timeline))
      (flush-batch changelog-batch (partial append-to-changelog timeline)))))

(defn- cache-store-key [f]
  [(:_collection f) (:_id f)])

(defn- cache-value [_ f] f)

(defn- cache-invalidate [_ f])

(defn- cache-use-key [collection id]
  [collection id])

(defn- batched-updater [{:keys [new-batch delete-for-update-batch flush-fn feature-cache]}]
  "Cache batched updater consisting of insert and delete"
  (let [batched-new (batched new-batch flush-fn)
        cache-batched-new (with-cache feature-cache batched-new cache-store-key cache-value)
        batched-delete (batched delete-for-update-batch flush-fn)]
    (fn [old-version old-valid-from old-valid-to f]
      (batched-delete [(:_collection f) old-version old-valid-from old-valid-to])
      (cache-batched-new f))))

(defn- batched-deleter [{:keys [delete-batch flush-fn feature-cache]}]
  "Cache batched deleter (thrasher) of timeline data"
  (let [batched-delete (batched delete-batch flush-fn)
        cache-remove (with-cache feature-cache identity cache-store-key cache-invalidate)]
    (fn [f]
      (cache-remove f)
      (let [collection (:_collection f)]
        (doseq [v (:_all_versions f)]
          (batched-delete [collection v]))))))

(defrecord Timeline [dataset db collections root-fn path-fn
                     feature-cache
                     delete-for-update-batch
                     new-batch
                     delete-batch
                     changelog-batch changelogs
                     make-flush-fn flush-fn
                     filestore]
  proj/Projector
  (proj/init [this for-dataset current-collections]
    (let [inited (assoc this :dataset for-dataset)
          flush-fn (make-flush-fn inited)
          inited (assoc inited :flush-fn flush-fn)]
      inited))
  (proj/new-collection [this collection parent-collection]
    (vswap! collections conj collection)
    (when-not parent-collection
      (init-collection db dataset collection)))
  (proj/flush [this]
    (flush-fn)
    this)
  (proj/new-feature [this feature]
    (let [[collection id] (feature-key feature)
          batched-new (batched new-batch flush-fn)
          cache-batched-new (with-cache feature-cache batched-new cache-store-key cache-value)
          cached-get-current (use-cache feature-cache cache-use-key)
          batched-append-changelog (batched changelog-batch flush-fn)
          current (cached-get-current collection id)]
      (when (not current) ;; Check idempotency
        (let [nw (sync-valid-from (merge (init-root collection id) feature) feature)]
          (cache-batched-new nw)
          (batched-append-changelog (changelog-new-entry nw))))))
  (proj/change-feature [this feature]
    (let [[collection id] (feature-key feature)
          batched-new (batched new-batch flush-fn)
          cache-batched-new (with-cache feature-cache batched-new cache-store-key cache-value)
          cache-batched-update (batched-updater this)
          cached-get-current (use-cache feature-cache cache-use-key)
          batched-append-changelog (batched changelog-batch flush-fn)
          current (cached-get-current collection id)]
      (when (and current (util/uuid> (:version feature) (:_version current))) ;; Check idempotency
        (let [new-current (merge current feature)]
          (if (t/before? (:_valid_from current) (:validity feature))
            (let [closed-current (sync-valid-to current feature)
                  ;; reset valid-to for new-current.
                  new-current+ (reset-valid-to (sync-valid-from new-current feature))]
              (cache-batched-update (:_version current) (:_valid_from current)
                                    (:_valid_to current) closed-current)
              (cache-batched-new new-current+)
              (batched-append-changelog (changelog-close-entry (:_version current) closed-current))
              (batched-append-changelog (changelog-new-entry new-current+)))
            (let [new-current+ (reset-valid-to (sync-valid-from new-current feature))]
              ;; reset valid-to because it might be closed because of nested features.
              (cache-batched-update (:_version current) (:_valid_from current)
                                    (:_valid_to current) new-current+)
              (batched-append-changelog (changelog-change-entry (:_version current) new-current+))))))))
  (proj/close-feature [this feature]
    (let [[collection id] (feature-key feature)
          cache-batched-update (batched-updater this)
          cached-get-current (use-cache feature-cache cache-use-key)
          batched-append-changelog (batched changelog-batch flush-fn)
          current (cached-get-current collection id)]
      (when (and current (util/uuid> (:version feature) (:_version current))) ;; Check idempotency
        (let [new-current (merge current feature)
              closed-or-new-current (sync-valid-to new-current feature)]
          (cache-batched-update (:_version current) (:_valid_from current)
                                (:_valid_to current) closed-or-new-current)
          (batched-append-changelog (changelog-close-entry (:_version current) closed-or-new-current))))))
  (proj/delete-feature [this feature]
    (let [[collection id] (feature-key feature)
          cache-batched-delete (batched-deleter this)
          cached-get-current (use-cache feature-cache cache-use-key)
          batched-append-changelog (batched changelog-batch flush-fn)
          current (cached-get-current collection id)]
      (when (and current (util/uuid> (:version feature) (:_version current))) ;; Check idempotency
        (cache-batched-delete current)
        (doseq [v (:_all_versions current)] ;; TODO is all versions still necessary?
          (batched-append-changelog (changelog-delete-entry (:_collection current) (:_id current) v))))))
  (proj/accept? [_ feature] true)
  (proj/close [this]
    (proj/flush this)
    this))

(defn create-cache [db persistence dataset chunk]
  (let [roots (map (fn [feature] (pers/root persistence (:collection feature) (:id feature))) chunk)
        per-c (group-by first roots)
        cache (volatile! (cache/basic-cache-factory {}))]
    (doseq [[collection roots-grouped-by] per-c]
      (apply-to-cache cache
                      (load-current-feature-cache db dataset collection (map second roots-grouped-by))))
    cache))

(defn process-chunk* [{:keys [config dataset collections changelogs filestore]} chunk]
  (let [cache (create-cache (:db-config config) (:persistence config) dataset chunk)
        timeline (proj/init (create config filestore cache dataset) dataset @collections)]
    (doseq [f chunk]
      (condp = (:action f)
        :new (proj/new-feature timeline f)
        :change (proj/change-feature timeline f)
        :close (proj/close-feature timeline f)
        :delete (proj/delete-feature timeline f)))
    (proj/close timeline)
    (swap! changelogs concat @(:changelogs timeline))))

(defn process-chunk [{:keys [chunk] :as chunked-timeline}]
  (with-bench t (log/debug "Processed chunk in" t "ms")
    (flush-batch chunk (partial process-chunk* chunked-timeline))))

(defrecord ChunkedTimeline [config dataset db collections chunk make-process-fn process-fn
                            changelogs filestore]
  proj/Projector
  (proj/init [this for-dataset current-collections]
    (let [inited (assoc this :dataset for-dataset)
          inited (assoc inited :process-fn (make-process-fn inited))
          _ (init db for-dataset)
          _ (vreset! collections current-collections)]
      (doseq [collection (filter #(nil? (:parent-collection %)) current-collections)]
        (init-collection db for-dataset (:name collection)))
      inited))
  (proj/new-collection [this collection parent-collection]
    (vswap! collections conj collection)
    (when-not parent-collection
      (init-collection db dataset collection)))
  (proj/flush [this]
    (process-chunk this)
    this)
  (proj/new-feature [_ feature] (process-fn feature))
  (proj/change-feature [_ feature] (process-fn feature))
  (proj/close-feature [_ feature] (process-fn feature))
  (proj/delete-feature [_ feature] (process-fn feature))
  (proj/close [this]
    (proj/flush this)
    this))

(defn create
  ([config filestore] (create config filestore (volatile! (cache/basic-cache-factory {})) "unknow-dataset"))
  ([config filestore cache for-dataset]
   (let [db (:db-config config)
         persistence (or (:persistence config) (pers/make-cached-jdbc-processor-persistence config))
         collections (volatile! #{})
         delete-for-update-batch (volatile! (PersistentQueue/EMPTY))
         new-batch (volatile! (PersistentQueue/EMPTY))
         delete-batch (volatile! (PersistentQueue/EMPTY))
         changelog-batch (volatile! (PersistentQueue/EMPTY))
         changelogs (atom [])
         make-flush-fn (make-flush-all new-batch delete-batch delete-for-update-batch changelog-batch)]
     (->Timeline for-dataset db collections
                 (partial pers/root persistence) (partial pers/path persistence)
                 cache
                 delete-for-update-batch
                 new-batch
                 delete-batch
                 changelog-batch changelogs
                 make-flush-fn (fn [])
                 filestore))))

(defn create-chunked [config filestore]
  (let [chunk-size (or (:chunk-size config) 10000)
        chunk (volatile! (PersistentQueue/EMPTY))
        make-process-fn (fn [tl] (batched chunk chunk-size #(process-chunk tl)))
        db (:db-config config)
        collections (volatile! #{})
        changelogs (atom [])]
    (->ChunkedTimeline config "unknown-dataset" db collections chunk make-process-fn (fn [])
                       changelogs filestore)))
