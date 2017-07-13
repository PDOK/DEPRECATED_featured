(ns pdok.featured.timeline
  (:require [pdok.cache :refer :all]
            [pdok.featured.dynamic-config :as dc]
            [pdok.postgres :as pg]
            [pdok.util :refer [with-bench checked] :as util]
            [pdok.featured.projectors :as proj]
            [pdok.transit :as transit]
            [pdok.filestore :as fs]
            [clojure.java.jdbc :as j]
            [clojure.core.cache :as cache]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [>!! close! chan]]
            [clojure.java.io :as io]
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

(defn- init [tx dataset]
  (when-not (pg/schema-exists? tx (schema dataset))
    (checked (pg/create-schema tx (schema dataset))
             (pg/schema-exists? tx (schema dataset)))))

(defn- init-collection [tx dataset collection]
  (let [table-name (str (name dc/*timeline-table*) "_" collection)]
    (with-bindings {#'dc/*timeline-schema* (schema dataset)}
      (when-not (pg/table-exists? tx dc/*timeline-schema* table-name)
        (checked (do (pg/create-table tx dc/*timeline-schema* table-name
                                      [:id "serial" :primary :key]
                                      [:feature_id "varchar(100)"]
                                      [:version "uuid"]
                                      [:valid_from "timestamp without time zone"]
                                      [:valid_to "timestamp without time zone"]
                                      [:feature "text"]
                                      [:tiles "integer[]"])
                     (pg/create-index tx dc/*timeline-schema* table-name :feature_id)
                     (pg/create-index tx dc/*timeline-schema* table-name :version))
                 (pg/table-exists? tx dc/*timeline-schema* table-name))))))

(defn current [tx dataset collection]
  (let [query (str "SELECT feature FROM " (qualified-timeline dataset collection) " WHERE valid_to IS NULL")]
    (try
      (let [results (j/query tx [query] :as-arrays? true)]
        (map (fn [[f]] (transit/from-json f)) (drop 1 results)))
      (catch SQLException e
        (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
        (throw e)))))

(defn keywordize-keys-and-remove-nils [m]
  "Recursively transforms all map keys from strings to keywords and removes all pairs with nil values."
  (let [f (fn [[k v]] (when (not (nil? v)) (if (string? k) [(keyword k) v] [k v])))]
    (walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) m)))

(defn- mustafy [feature]
  (into {} (for [[k v] (:attributes feature)]
             (when v
               [(keyword k) (keywordize-keys-and-remove-nils v)]))))

(defn- merge* [target current-versions feature]
  (merge target
         {:_collection (:collection feature)
          :_id (:id feature)
          :_version (:version feature)
          :_all_versions (conj current-versions (:version feature))
          :_tiles (or (:_tiles target) (:tiles feature))})) ;; Current tiles for close, new tiles for new and change.

(defn build-new-entry [feature]
  (-> (merge* (mustafy feature) (list) feature)
      (assoc :_valid_from (:validity feature))))

(defn build-change-entry [current feature]
  (-> (merge* (mustafy feature) (:_all_versions current) feature)
      (assoc :_valid_from (:validity feature))))

(defn build-close-entry [current feature]
  (-> (merge* current (:_all_versions current) feature)
      (assoc :_valid_to (:validity feature))))

;;(def feature-info (juxt :_collection :_id #(.toString (:_valid_from %)) #(when (:_valid_to %1) (.toString (:_valid_to %1))) :_version))

(defn- new-current [{:keys [tx dataset]} features]
  ;(println "NEW-CURRENT" (map feature-info features))
  (try
    (let [per-collection (group-by :_collection features)]
      (doseq [[collection collection-features] per-collection]
        (let [transform-fn (juxt :_id :_version :_valid_from :_valid_to transit/to-json :_tiles)
              records (map transform-fn collection-features)]
          (pg/batch-insert tx (qualified-timeline dataset collection)
                           [:feature_id, :version, :valid_from, :valid_to, :feature, :tiles] records))))
    (catch SQLException e
      (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
      (throw e))))

(defn- load-current-feature-cache-sql [dataset collection n]
  (str "SELECT DISTINCT ON (feature_id) feature_id, feature"
       " FROM " (qualified-timeline dataset collection)
       " WHERE feature_id in (" (clojure.string/join "," (repeat n "?")) ")"
       " ORDER BY feature_id ASC, valid_from DESC"))

(defn- load-current-feature-cache [tx dataset collection ids]
  (try
    (let [results (j/query tx (apply vector (load-current-feature-cache-sql dataset collection (count ids)) ids)
                           :as-arrays? true)]
      (map (fn [[fid f]] [[collection fid] (transit/from-json f)]) (drop 1 results)))
    (catch SQLException e
      (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
      (throw e))))

(defn- delete-version-with-valid-from-sql [dataset collection]
  (str "DELETE FROM " (qualified-timeline dataset collection)
       " WHERE version = ? AND valid_from = ? AND valid_to IS NULL"))

(defn- delete-feature-with-version [{:keys [tx dataset]} records]
  "([collection version] ... )"
  (let [per-collection (group-by first records)]
    (doseq [[collection collection-records] per-collection]
      (let [versions (map #(vector (nth % 1)) collection-records)]
        (when (seq versions)
          (try
            (pg/batch-delete tx (qualified-timeline dataset collection) [:version] versions)
            (catch SQLException e
              (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
              (throw e))))))))

(defn- delete-feature-with-version-and-validity [{:keys [tx dataset]} records]
  "([collection version valid-from valid-to] ... )"
  (let [per-collection (group-by first records)]
    (doseq [[collection collection-records] per-collection]
      (let [{open true closed false} (group-by (fn [[_ _ valid-to]] (nil? valid-to)) (map rest collection-records))]
        (when (seq open)
          (try
            ;; Cannot use pg/batch-delete, because "valid_to = NULL" is not the same as "valid_to IS NULL".
            (pg/execute-batch-query tx (delete-version-with-valid-from-sql dataset collection) (map #(take 2 %) open))
            (catch SQLException e
              (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
              (throw e))))
        (when (seq closed)
          (try
            (pg/batch-delete tx (qualified-timeline dataset collection) [:version :valid_from :valid_to] closed)
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

(defrecord Timeline [dataset tx collections feature-cache delete-for-update-batch new-batch delete-batch changelog-batch
                     changelogs make-flush-fn flush-fn filestore]
  proj/Projector
  (proj/init [this transaction for-dataset current-collections]
    (let [inited (-> this (assoc :dataset for-dataset) (assoc :tx transaction))
          flush-fn (make-flush-fn inited)
          inited (assoc inited :flush-fn flush-fn)]
      inited))
  (proj/new-collection [this collection]
    (vswap! collections conj collection)
    (init-collection tx dataset collection))
  (proj/flush [this]
    (flush-fn)
    this)
  (proj/new-feature [this feature]
    (let [batched-new (batched new-batch flush-fn)
          cache-batched-new (with-cache feature-cache batched-new cache-store-key cache-value)
          cached-get-current (use-cache feature-cache cache-use-key)
          batched-append-changelog (batched changelog-batch flush-fn)
          current (cached-get-current (:collection feature) (:id feature))]
      (if (not current)
        ;; "Real" new
        (let [new-entry (build-new-entry feature)]
          (cache-batched-new new-entry)
          (batched-append-changelog (changelog-new-entry new-entry)))
        ;; new after close
        (when (and current (util/uuid> (:version feature) (:_version current))) ;; Check idempotency
          (let [new-entry (build-change-entry current feature)]
            (cache-batched-new new-entry)
            (batched-append-changelog (changelog-new-entry new-entry)))))))
  (proj/change-feature [this feature]
    (let [cache-batched-update (batched-updater this)
          cached-get-current (use-cache feature-cache cache-use-key)
          batched-append-changelog (batched changelog-batch flush-fn)
          current (cached-get-current (:collection feature) (:id feature))]
      (when (and current (util/uuid> (:version feature) (:_version current))) ;; Check idempotency
        (let [change-entry (build-change-entry current feature)]
          (cache-batched-update (:_version current) (:_valid_from current)
                                (:_valid_to current) change-entry)
          (batched-append-changelog (changelog-change-entry (:_version current) change-entry))))))
  (proj/close-feature [this feature]
    (let [cache-batched-update (batched-updater this)
          cached-get-current (use-cache feature-cache cache-use-key)
          batched-append-changelog (batched changelog-batch flush-fn)
          current (cached-get-current (:collection feature) (:id feature))]
      (when (and current (util/uuid> (:version feature) (:_version current))) ;; Check idempotency
        (let [close-entry (build-close-entry current feature)]
          (cache-batched-update (:_version current) (:_valid_from current)
                                (:_valid_to current) close-entry)
          (batched-append-changelog (changelog-close-entry (:_version current) close-entry))))))
  (proj/delete-feature [this feature]
    (let [cache-batched-delete (batched-deleter this)
          cached-get-current (use-cache feature-cache cache-use-key)
          batched-append-changelog (batched changelog-batch flush-fn)
          current (cached-get-current (:collection feature) (:id feature))]
      (when (and current (util/uuid> (:version feature) (:_version current))) ;; Check idempotency
        (cache-batched-delete current)
        (doseq [v (:_all_versions current)] ;; TODO only delete what can actually exist in the database
          (batched-append-changelog (changelog-delete-entry (:_collection current) (:_id current) v))))))
  (proj/accept? [_ feature] true)
  (proj/close [this]
    (proj/flush this)
    this))

(defn create-cache [tx dataset chunk]
  (let [pairs (map (fn [feature] [(:collection feature) (:id feature)]) chunk)
        per-c (group-by first pairs)
        cache (volatile! (cache/basic-cache-factory {}))]
    (doseq [[collection roots-grouped-by] per-c]
      (apply-to-cache cache
                      (load-current-feature-cache tx dataset collection (map second roots-grouped-by))))
    cache))

(defn process-chunk* [{:keys [tx dataset collections changelogs filestore]} chunk]
  (let [cache (create-cache tx dataset chunk)
        timeline (proj/init (create filestore cache dataset) tx dataset @collections)]
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

(defrecord ChunkedTimeline [config dataset tx collections chunk make-process-fn process-fn changelogs filestore]
  proj/Projector
  (proj/init [this transaction for-dataset current-collections]
    (let [inited (-> this (assoc :dataset for-dataset) (assoc :tx transaction))
          inited (assoc inited :process-fn (make-process-fn inited))
          _ (init transaction for-dataset)
          _ (vreset! collections current-collections)]
      (doseq [collection current-collections]
        (init-collection transaction for-dataset (:name collection)))
      inited))
  (proj/new-collection [this collection]
    (vswap! collections conj collection)
    (init-collection tx dataset collection))
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
  ([filestore] (create filestore (volatile! (cache/basic-cache-factory {})) "unknown-dataset"))
  ([filestore cache for-dataset]
   (let [collections (volatile! #{})
         delete-for-update-batch (volatile! (PersistentQueue/EMPTY))
         new-batch (volatile! (PersistentQueue/EMPTY))
         delete-batch (volatile! (PersistentQueue/EMPTY))
         changelog-batch (volatile! (PersistentQueue/EMPTY))
         changelogs (atom [])
         make-flush-fn (make-flush-all new-batch delete-batch delete-for-update-batch changelog-batch)]
     (->Timeline for-dataset nil collections cache delete-for-update-batch new-batch delete-batch changelog-batch
                 changelogs make-flush-fn (fn []) filestore))))

(defn create-chunked [config filestore]
  (let [chunk-size (or (:chunk-size config) 10000)
        chunk (volatile! (PersistentQueue/EMPTY))
        make-process-fn (fn [tl] (batched chunk chunk-size #(process-chunk tl)))
        collections (volatile! #{})
        changelogs (atom [])]
    (->ChunkedTimeline config "unknown-dataset" nil collections chunk make-process-fn (fn []) changelogs filestore)))
