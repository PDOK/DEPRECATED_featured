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
            [clojure.zip :as zip]
            [clj-time.core :as t]
            [clojure.core.async :as a
             :refer [>!! close! chan]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as walk])
  (:import [clojure.lang MapEntry PersistentQueue]
           (java.io OutputStream)
           (java.sql SQLException)
           (java.util.zip ZipEntry ZipOutputStream)))

(declare create)

(defn indices-of [f coll]
  (keep-indexed #(if (f %2) %1 nil) coll))

(defn index-of [f coll]
  (first (indices-of f coll)))

(defn schema [dataset]
  (str (name dc/*timeline-schema-prefix*) "_" dataset))

(defn- qualified-timeline [dataset collection]
  (str (pg/quoted (schema dataset)) "." (pg/quoted (str (name dc/*timeline-table*) "_" collection))))

(defn- qualified-changelog [dataset]
  (str (pg/quoted (schema dataset)) "." (name dc/*timeline-changelog*)))

(defn- init [db dataset]
  (when-not (pg/schema-exists? db (schema dataset))
    (checked (pg/create-schema db (schema dataset))
             (pg/schema-exists? db (schema dataset))))
  (with-bindings {#'dc/*timeline-schema* (schema dataset)}
    (when-not (pg/table-exists? db dc/*timeline-schema* dc/*timeline-changelog*)
      (checked (do (pg/create-table db dc/*timeline-schema* dc/*timeline-changelog*
                                    [:id "serial" :primary :key]
                                    [:collection "varchar(255)"]
                                    [:feature_id "varchar(100)"]
                                    [:old_version "uuid"]
                                    [:version "uuid"]
                                    [:valid_from "timestamp without time zone"]
                                    [:action "varchar(12)"]
                                    )
                   (pg/create-index db dc/*timeline-schema* dc/*timeline-changelog* :version :action)
                   (pg/create-index db dc/*timeline-schema* dc/*timeline-changelog* :collection))
               (pg/table-exists? db dc/*timeline-schema* dc/*timeline-changelog*)))))

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

(defn closed
  ([{:keys [dataset] :as timeline} collection]
   (let [query (str "SELECT feature FROM " (qualified-timeline dataset collection) " "
                    "WHERE valid_to is not null")]
     (execute-query timeline query))))

(defn- record->result [fn-transform c record]
  (>!! c (fn-transform record)))

(defn- query-with-results-on-channel [db-config query fn-transform]
  "Returns a channel which contains the (transformed) results of the query"
  (let [rc (chan)]
    (a/thread
      (j/with-db-connection [c (:db db-config)]
        (let [statement (j/prepare-statement
                         (doto (j/get-connection c) (.setAutoCommit false))
                         query :fetch-size 10000)]
          (j/query c [statement] :row-fn (partial record->result fn-transform rc))
          (close! rc))))
    rc))

(defn- upgrade-changelog [changelog]
  (-> changelog
      (update-in [:feature] transit/from-json)
      (update-in [:action] keyword)))

(defn changed-features [{:keys [dataset] :as timeline} collection]
   (let [query (str "SELECT cl.collection, cl.feature_id, cl.old_version, cl.version, cl.action, cl.valid_from, tl.feature
 FROM " (qualified-changelog dataset) " AS cl
 LEFT JOIN " (qualified-timeline dataset collection) " AS tl
 ON cl.version = tl.version AND cl.valid_from = tl.valid_from
 ORDER BY cl.id ASC")]
     (query-with-results-on-channel timeline query upgrade-changelog)))

(defn all-features [{:keys [dataset] :as timeline} collection]
  (let [query (str "SELECT feature_id, NULL as old_version, version, 'new' as action, valid_from, feature
                    FROM " (qualified-timeline dataset collection))]
     (query-with-results-on-channel timeline query upgrade-changelog)))

(defn delete-changelog [{:keys [db dataset]}]
  (try
    (j/delete! db (qualified-changelog dataset) [])
  (catch SQLException e
    (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
    (throw e))))

(defn collections-in-changelog [{:keys [db dataset]}]
  (let [sql (str "SELECT DISTINCT collection FROM " (qualified-changelog dataset)) ]
    (try
      (flatten (drop 1 (j/query db [sql] :as-arrays? true)))
       (catch SQLException e
         (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
         (throw e)))))


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

(defn drop-nth [v n]
  (into [] (concat (subvec v 0 n) (subvec v (inc n) (count v)))))

(defn mapvec-zipper [m]
  (zip/zipper
   (fn [x] (or
           (when (vector? x) (vector? (nth x 1)))
           (map? x)))
   (fn [x] (cond
            (map? x) (seq x)
            (vector? x) (nth x 1)))
   (fn [node children]
     (cond
       (instance? MapEntry node) (MapEntry. (first node) (vec children))
       (map? node) (into {} children)
       (vector? node) children))
    m))

(defn zip-find-key [zipper key]
  (when zipper
    (loop [inner (zip/down zipper)]
      (when inner
        (let [[[k v] _] inner]
          (if-not (= k key)
            (recur (zip/right inner))
            inner))))))

(defn zip-filter-seq [zipper filter-fn]
  (when zipper
    (loop [inner (zip/down zipper)]
      (when inner
        (if-not (filter-fn (zip/node inner))
          (recur (zip/right inner))
          inner)))))

(defn insert* [target path feature]
  (if (seq path)
    (loop [zipper (mapvec-zipper target)
           [[field id] & more] path]
      (let [loc (zip-find-key zipper field)]
        (cond
          (and loc (not (vector? (nth (zip/node loc) 1))))
          (insert* (zip/root (zip/remove loc)) path feature)
          loc
          (if-let [nested (zip-filter-seq loc #(= id (:_id %)))]
            (if-not more
              (zip/root (zip/replace nested (clojure.core/merge (zip/node nested) feature)))
              (recur nested more))
            (if-not more
              (zip/root (zip/insert-child loc feature))
              (recur (-> (zip/insert-child loc zip/down)) more)))
          ;; add root and step into it
          :else
          (if-not more
            (zip/root (zip/insert-child zipper (MapEntry. field [feature])))
            (recur (-> (zip/insert-child zipper (MapEntry. field [{}]))
                       zip/down zip/down)
                   more)))))
    (clojure.core/merge target feature)))

(defn delete-in* [target path]
  (if (seq path)
    (loop [zipper (mapvec-zipper target)
           [[field id] & more] path]
      (if-let [loc (zip-find-key zipper field)]
        (cond
          (or (not (vector? (nth (zip/node loc) 1))) (not (seq (nth (zip/node loc) 1))))
          target
          :else
          (if-let [nested (zip-filter-seq loc #(= id (:_id %)))]
            (if more
              (recur nested more)
              (zip/root (zip/remove nested)))
            target))
        target))
    target))

(defn merge* [target action path feature]
  (condp = action
    :close (delete-in* target path)
    (insert* target path feature)))

(defn merge
  ([target path feature]
   (let [keyworded-path (map (fn [[_ _ field id]] [(keyword field) id]) path)
         mustafied (mustafy feature)
         merged (merge* target (:action feature) keyworded-path mustafied)
         merged (assoc merged :_version (:version feature))
         merged (update-in merged [:_all_versions] (fnil conj '()) (:version feature))
         merged (update merged :_tiles #((fnil clojure.set/union #{}) % (:tiles feature)))]
     merged)))

(defn- sync-valid-from [acc feature]
  (assoc acc :_valid_from  (:validity feature)))

(defn- sync-version [acc feature]
  (-> acc
      (assoc :_version (first (:_all_versions acc)))
      (update-in [:_all_versions] (fnil conj '()) (:version feature))
      (update-in [:_all_versions] distinct)))

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

(defn- append-to-db-changelog-sql [dataset]
  (str "INSERT INTO " (qualified-changelog dataset)
       " (collection, feature_id, old_version, version, valid_from, action)
 SELECT ?, ?, ?, ?, ?, ?
 WHERE NOT EXISTS (SELECT 1 FROM " (qualified-changelog dataset)
 " WHERE version = ? AND action = ?)"))

(defn- append-to-db-changelog [{:keys [db dataset]} log-entries]
  "([collection id old-version version valid_from action] .... )"
  (try
    (let [transform-fn  (fn [rec] (let [[_ _ ov v a] rec] (conj rec v a)))
          records (map transform-fn log-entries)]
      (j/execute! db (cons (append-to-db-changelog-sql dataset) records) :multi? true :transaction? (:transaction? db)))
    (catch SQLException e
      (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))
      (throw e))))

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

(defn- make-flush-all [new-batch delete-batch delete-for-update-batch db-changelog-batch changelog-batch]
  "Used for flushing all batches, so update current is always performed after new current"
  (fn [timeline]
    (fn []
      (flush-batch new-batch (partial new-current timeline))
      (flush-batch delete-for-update-batch (partial delete-feature-with-version-and-validity timeline))
      (flush-batch delete-batch (partial delete-feature-with-version timeline))
      (flush-batch db-changelog-batch (partial append-to-db-changelog timeline))
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

(defn- create-changelog-entry [feature]
  [(:_collection feature) (:_id feature)])

(defrecord Timeline [dataset db collections root-fn path-fn
                     feature-cache
                     delete-for-update-batch
                     new-batch
                     delete-batch
                     db-changelog-batch
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
          [root-col root-id] (root-fn collection id)
          path (path-fn collection id)
          batched-new (batched new-batch flush-fn)
          cache-batched-new (with-cache feature-cache batched-new cache-store-key cache-value)
          cache-batched-update (batched-updater this)
          cached-get-current (use-cache feature-cache cache-use-key)
          batched-append-db-changelog (batched db-changelog-batch flush-fn)
          batched-append-changelog (batched changelog-batch flush-fn)]
      (if-let [current (cached-get-current root-col root-id)]
        (when (util/uuid> (:version feature) (:_version current))
          (let [new-current (merge current path feature)]
            (if (= (:action feature) :close)
              ;; Process close of child as change of parent.
              (let [collection-is-root (= collection root-col)
                    closed-or-new-current (if collection-is-root (sync-valid-to new-current feature) new-current)
                    action (if collection-is-root :close :change)
                    changelog-entry-fn (if collection-is-root changelog-close-entry changelog-change-entry)]
                (cache-batched-update (:_version current) (:_valid_from current)
                                      (:_valid_to current) closed-or-new-current)
                (batched-append-db-changelog [(:_collection new-current)
                                              (:_id new-current) (:_version current)
                                              (:_version new-current) (:_valid_from new-current) action])
                (batched-append-changelog (changelog-entry-fn (:_version current) closed-or-new-current)))
              (if (t/before? (:_valid_from current) (:validity feature))
                (let [closed-current (sync-valid-to current feature)
                      ;; reset valid-to for new-current.
                      new-current+ (reset-valid-to (sync-valid-from new-current feature))]
                  (cache-batched-update (:_version current) (:_valid_from current)
                                        (:_valid_to current) closed-current)
                  (cache-batched-new new-current+)
                  (batched-append-db-changelog [(:_collection new-current)
                                                (:_id new-current) (:_version current) (:_version current)
                                                (:_valid_from current) :close])
                  (batched-append-db-changelog [(:_collection new-current)
                                                (:_id new-current) nil
                                                (:_version new-current) (:validity feature) :new])
                  (batched-append-changelog (changelog-close-entry (:_version current) closed-current))
                  (batched-append-changelog (changelog-new-entry new-current+)))
                (let [new-current+ (reset-valid-to (sync-valid-from new-current feature))]
                  ;; reset valid-to because it might be closed because of nested features.
                  (cache-batched-update (:_version current) (:_valid_from current)
                                        (:_valid_to current) new-current+)
                  (batched-append-db-changelog [(:_collection new-current)
                                                (:_id new-current) (:_version current)
                                                (:_version new-current) (:validity feature) :change])
                  (batched-append-changelog (changelog-change-entry (:_version current) new-current+)))))))
        (let [nw (sync-valid-from (merge (init-root root-col root-id) path feature) feature)]
          (cache-batched-new nw)
          (batched-append-db-changelog [(:_collection nw)
                                     (:_id nw) nil
                                     (:_version nw) (:validity feature) :new])
          (batched-append-changelog (changelog-new-entry nw))))))
  (proj/change-feature [this feature]
    ;; change can be the same, because a new nested feature validity change will also result in a new validity
    ;(println "CHANGE")
    (proj/new-feature this feature))
  (proj/close-feature [this feature]
    (proj/new-feature this feature))
  (proj/delete-feature [this feature]
    (let [[collection id] (feature-key feature)
          [root-col root-id] (root-fn collection id)
          cache-batched-delete (batched-deleter this)
          cached-get-current (use-cache feature-cache cache-use-key)
          batched-append-db-changelog (batched db-changelog-batch flush-fn)
          batched-append-changelog (batched changelog-batch flush-fn)]
      (when-let [current (cached-get-current root-col root-id)]
        (when (util/uuid> (:version feature) (:_version current))
          (cache-batched-delete current)
          (doseq [v (:_all_versions current)]
            (batched-append-db-changelog [(:_collection current)
                                       (:_id current) nil
                                       v (:validity current) :delete])
            (batched-append-changelog (changelog-delete-entry (:_collection current) (:_id current) v)))))))
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
         db-changelog-batch (volatile! (PersistentQueue/EMPTY))
         changelog-batch (volatile! (PersistentQueue/EMPTY))
         changelogs (atom [])
         make-flush-fn (make-flush-all new-batch delete-batch delete-for-update-batch
                                       db-changelog-batch changelog-batch)]
     (->Timeline for-dataset db collections
                 (partial pers/root persistence) (partial pers/path persistence)
                 cache
                 delete-for-update-batch
                 new-batch
                 delete-batch
                 db-changelog-batch
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
