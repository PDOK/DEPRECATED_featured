(ns pdok.featured.timeline
  (:refer-clojure :exclude [merge])
  (:require [pdok.cache :refer :all]
            [pdok.featured.dynamic-config :as dc]
            [pdok.postgres :as pg]
            [pdok.util :refer [with-bench] :as util]
            [pdok.featured.projectors :as proj]
            [pdok.featured.persistence :as pers]
            [pdok.featured.tiles :as tiles]
            [clojure.java.jdbc :as j]
            [clojure.core.cache :as cache]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.zip :as zip]
            [clj-time.core :as t]
            [clojure.core.async :as a
             :refer [>!! close! chan]])
  (:import [clojure.lang MapEntry]))

(declare create)

(defn indices-of [f coll]
  (keep-indexed #(if (f %2) %1 nil) coll))

(defn index-of [f coll]
  (first (indices-of f coll)))

(defn schema [dataset]
  (str (name dc/*timeline-schema-prefix*) "_" dataset))

(defn- qualified-history [dataset]
  (str (pg/quoted (schema dataset)) "." (name dc/*timeline-history-table*)))

(defn- qualified-current [dataset]
  (str (pg/quoted (schema dataset)) "." (name dc/*timeline-current-table*)))

(defn- qualified-changelog [dataset]
  (str (pg/quoted (schema dataset)) "." (name dc/*timeline-changelog*)))

(defn- init [db dataset]
  (when-not (pg/schema-exists? db (schema dataset))
    (pg/create-schema db (schema dataset)))
  (with-bindings {#'dc/*timeline-schema* (schema dataset)}
    (when-not (pg/table-exists? db dc/*timeline-schema* dc/*timeline-history-table*)
      (pg/create-table db dc/*timeline-schema* dc/*timeline-history-table*
                       [:id "serial" :primary :key]
                       [:collection "varchar(255)"]
                       [:feature_id "varchar(100)"]
                       [:version "uuid"]
                       [:valid_from "timestamp without time zone"]
                       [:valid_to "timestamp without time zone"]
                       [:feature "text"]
                       [:tiles "integer[]"])
      (pg/create-index db dc/*timeline-schema* dc/*timeline-history-table* :collection :feature_id)
      (pg/create-index db dc/*timeline-schema* dc/*timeline-history-table* :version))
    (when-not (pg/table-exists? db dc/*timeline-schema* dc/*timeline-current-table*)
      (pg/create-table db dc/*timeline-schema* dc/*timeline-current-table*
                       [:id "serial" :primary :key]
                       [:collection "varchar(255)"]
                       [:feature_id "varchar(100)"]
                       [:version "uuid"]
                       [:valid_from "timestamp without time zone"]
                       [:valid_to "timestamp without time zone"]
                       [:feature "text"]
                       [:tiles "integer[]"])
      (pg/create-index db dc/*timeline-schema* dc/*timeline-current-table* :collection :feature_id)
      (pg/create-index db dc/*timeline-schema* dc/*timeline-current-table* :version))
    (when-not (pg/table-exists? db dc/*timeline-schema* dc/*timeline-changelog*)
      (pg/create-table db dc/*timeline-schema* dc/*timeline-changelog*
                       [:id "serial" :primary :key]
                       [:collection "varchar(255)"]
                       [:feature_id "varchar(100)"]
                       [:old_version "uuid"]
                       [:version "uuid"]
                       [:valid_from "timestamp without time zone"]
                       [:action "varchar(12)"]
                       )
      (pg/create-index db dc/*timeline-schema* dc/*timeline-changelog* :version :action)
      (pg/create-index db dc/*timeline-schema* dc/*timeline-changelog* :collection))))

(defn- execute-query [timeline query]
  (try (j/with-db-connection [c (:db timeline)]
         (let [results (j/query c [query] :as-arrays? true)
               results (map (fn [[f]] (pg/from-json f)) (drop 1 results))]
           results))
       (catch java.sql.SQLException e
          (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e)))))

(defn current
  ([{:keys [dataset] :as timeline} collection]
   (current timeline {:dataset dataset :collection collection}))
  ([timeline selector _]
   (let [query (str "SELECT feature FROM " (qualified-current) " "
                    "WHERE valid_to is null AND " (pg/map->where-clause selector))]
     (execute-query timeline query))))

(defn closed
  ([{:keys [dataset] :as timeline} collection]
   (closed timeline {:dataset dataset :collection collection}))
  ([timeline selector _]
   (let [query (str "SELECT feature FROM " (qualified-current) " "
                    "WHERE valid_to is not null AND " (pg/map->where-clause selector))]
     (execute-query timeline query))))

(defn history
  ([{:keys [dataset] :as timeline} collection]
   (history timeline {:dataset dataset :collection collection}))
  ([timeline selector _]
   (let [query (str "SELECT feature FROM " (qualified-history) " "
                    "WHERE " (pg/map->where-clause selector))]
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
      (update-in [:feature] pg/from-json)
      (update-in [:action] keyword)))

(defn changed-features [{:keys [dataset] :as timeline} collection]
   (let [query (str "SELECT cl.collection, cl.feature_id, cl.old_version, cl.version, cl.action, cl.valid_from, tl.feature
 FROM " (qualified-changelog dataset) " AS cl
 LEFT JOIN
(SELECT collection, feature_id, version, valid_from, feature  FROM "
 (qualified-current dataset)
" UNION ALL
 SELECT collection, feature_id, version, valid_from, feature FROM "
(qualified-history dataset)
") as tl ON cl.collection = tl.collection AND cl.version = tl.version
 WHERE cl.collection = '" collection "'
 ORDER BY cl.id ASC")]
     (query-with-results-on-channel timeline query upgrade-changelog)))

(defn all-features [{:keys [dataset] :as timeline} collection]
  (let [query (str "SELECT collection, feature_id, NULL as old_version, version, 'new' as action, valid_from, feature
                    FROM " (qualified-current dataset) "
                    WHERE collection = '" collection "'
                    UNION ALL
                    SELECT collection, feature_id, NULL as old_version, version, 'new' as action, valid_from, feature
                    FROM " (qualified-history dataset) "
                    WHERE collection = '" collection "'")]
     (query-with-results-on-channel timeline query upgrade-changelog)))

(defn delete-changelog [{:keys [db dataset]}]
  (try
    (j/delete! db (qualified-changelog dataset) [])
  (catch java.sql.SQLException e
    (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e)))))

(defn collections-in-changelog [{:keys [db dataset]}]
  (let [sql (str "SELECT DISTINCT collection FROM " (qualified-changelog dataset)) ]
    (try
      (flatten (drop 1 (j/query db [sql] :as-arrays? true)))
       (catch java.sql.SQLException e
         (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))))))


(defn- init-root
  ([feature]
   (init-root (:collection feature) (:id feature)))
  ([collection id]
   {:_collection collection :_id id }))

(defn- mustafy [feature]
  (let [result (init-root feature)
        geom (:geometry feature)
        result (cond-> result geom (assoc :_geometry geom))
        result (reduce (fn [acc [k v]] (assoc acc (keyword k) v)) result (:attributes feature))]
    result))

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
              (recur (-> (zip/insert-child loc) zip/down) more)))
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
         merged (update merged :_tiles #((fnil clojure.set/union #{}) % (tiles/nl (:geometry feature))))]
     merged)))

(defn- sync-valid-from [acc feature]
  (assoc acc :_valid_from  (:validity feature)))

(defn- sync-version [acc feature]
  (assoc acc :_version (:current-version feature)))

(defn- sync-valid-to [acc feature]
  (let [changed (assoc acc :_valid_to (:validity feature))]
    changed))

(defn- reset-valid-to [acc]
  (assoc acc :_valid_to nil))

(defn- new-current-sql [dataset]
  (str "INSERT INTO " (qualified-current dataset)
       " (collection, feature_id, version, valid_from, valid_to, feature, tiles)
VALUES (?, ? ,?, ?, ?, ?, ?)"))

(defn- new-current
  ([{:keys [db dataset]} features]
   (try
     (let [transform-fn (juxt :_collection :_id :_version
                              :_valid_from :_valid_to pg/to-json :_tiles)
           records (map transform-fn features)
           versions (map (juxt :_collection :_version :_version) features)]
       (j/execute! db (cons (new-current-sql dataset) records) :multi? true :transaction? (:transaction? db)))
     (catch java.sql.SQLException e
       (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))))))

(defn- load-current-feature-cache-sql [dataset n]
  (str "SELECT collection, feature_id, feature  FROM " (qualified-current dataset)
       " WHERE collection = ? AND feature_id in ("
       (clojure.string/join "," (repeat n "?")) ")")
  )

(defn- load-current-feature-cache [db dataset collection ids]
  (try (j/with-db-connection [c db]
         (let [results
               (j/query c (apply vector (load-current-feature-cache-sql dataset (count ids))
                                 collection ids) :as-arrays? true)
               for-cache
               (map (fn [[col fid f]] [[col fid] (pg/from-json f)] ) (drop 1 results))]
           for-cache))
       (catch java.sql.SQLException e
          (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e)))))

(defn- delete-current-sql [dataset]
  (str "DELETE FROM " (qualified-current dataset)
       " WHERE version = ?"))

(defn- delete-current [{:keys [db dataset]} records]
  "([collection version version] ... )"
  (let [versions (map #(vector (get % 1)) records)]
    (try
      (j/execute! db (cons (delete-current-sql dataset) versions) :multi? true :transaction? (:transaction? db))
     (catch java.sql.SQLException e
       (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e))))))

(defn- new-history-sql [dataset]
  (str "INSERT INTO " (qualified-history dataset) " (collection, feature_id, version, valid_from, valid_to, feature, tiles)
VALUES (?, ?, ?, ?, ?, ?, ?)"))

(defn- new-history [{:keys [db dataset]} features]
  (try
    (let [transform-fn (juxt :_collection :_id :_version :_valid_from :_valid_to pg/to-json :_tiles)
          records (map transform-fn features)
          versions (map (juxt :_collection :_version :_version) features)]
      (j/execute! db (cons (new-history-sql dataset) records) :multi? true :transaction? (:transaction? db)))
     (catch java.sql.SQLException e
       (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e)))))

(defn- delete-history-sql [dataset]
  (str "DELETE FROM " (qualified-history dataset)
       " WHERE version = ?"))

(defn- delete-history [{:keys [db dataset]} features]
  (try
    (let [records (map vector (mapcat :_all_versions features))]
      (j/execute! db (cons (delete-history-sql dataset) records) :multi? true :transaction? (:transaction? db)))
     (catch java.sql.SQLException e
       (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e)))))

(defn- append-to-changelog-sql [dataset]
  (str "INSERT INTO " (qualified-changelog dataset)
       " (collection, feature_id, old_version, version, valid_from, action)
 SELECT ?, ?, ?, ?, ?, ?
 WHERE NOT EXISTS (SELECT 1 FROM " (qualified-changelog dataset)
 " WHERE version = ? AND action = ?)"))

(defn- append-to-changelog [{:keys [db dataset]} log-entries]
  "([collection id old-version version valid_from action] .... )"
  (try
    (let [transform-fn  (fn [rec] (let [[_ _ ov v a] rec] (conj rec v a)))
          records (map transform-fn log-entries)]
      (j/execute! db (cons (append-to-changelog-sql dataset) records) :multi? true :transaction? (:transaction? db)))
    (catch java.sql.SQLException e
      (log/with-logs ['pdok.featured.timeline :error :error] (j/print-sql-exception-chain e)))))

(defn- feature-key [feature]
  [(:collection feature) (:id feature)])

(defn- make-flush-all [new-current-batch delete-current-batch new-history-batch
                  delete-history-batch changelog-batch]
  "Used for flushing all batches, so update current is always performed after new current"
  (fn [timeline]
    (fn []
      (flush-batch new-current-batch (partial new-current timeline))
      (flush-batch delete-current-batch (partial delete-current timeline))
      (flush-batch new-history-batch (partial new-history timeline))
      (flush-batch delete-history-batch (partial delete-history timeline))
      (flush-batch changelog-batch (partial append-to-changelog timeline)))))

(defn- cache-store-key [f]
  [(:_collection f) (:_id f)])

(defn- cache-value [_ f] f)

(defn- cache-invalidate [_ f])

(defn- cache-use-key [collection id]
  [collection id])

(defn- batched-updater [timeline ncb ncb-size dcb dcb-size flush-fn feature-cache]
  "Cache batched updater consisting of insert and delete"
  (let [batched-new (batched ncb ncb-size flush-fn)
        cache-batched-new (with-cache feature-cache batched-new cache-store-key cache-value)
        batched-delete (batched dcb dcb-size flush-fn)]
    (fn [f]
      (cache-batched-new f)
      (let [version (first (rest (:_all_versions f)))]
        (batched-delete [(:_collection f) version version])))))

(defn- batched-deleter [timeline dcb dcb-size dhb dhb-size flush-fn feature-cache]
  "Cache batched deleter (thrasher) of current and history"
  (let [batched-delete-cur (batched dcb dcb-size flush-fn)
        cache-remove-cur (with-cache feature-cache identity cache-store-key cache-invalidate)
        batched-delete-his (batched dhb dhb-size flush-fn)]
    (fn [f]
      (cache-remove-cur f)
      (batched-delete-cur [(:_collection f) (:_version f) (:_version f)])
      (batched-delete-his f))))

(defn- create-changelog-entry [feature]
  [(:_collection feature) (:_id feature)])

(defrecord Timeline [dataset db root-fn path-fn
                     feature-cache
                     new-current-batch new-current-batch-size
                     delete-current-batch delete-current-batch-size
                     new-history-batch new-history-batch-size
                     delete-history-batch delete-history-batch-size
                     changelog-batch changelog-batch-size
                     make-flush-fn flush-fn]
  proj/Projector
  (proj/init [this for-dataset]
    (let [inited (assoc this :dataset for-dataset)
          flush-fn (make-flush-fn inited)
          inited (assoc inited :flush-fn flush-fn)
          ;;_ (init db for-dataset) not needed if we only go through chunked-timeline
          ]
      inited))
  (proj/flush [this]
    (flush-fn)
    this)
  (proj/new-feature [this feature]
    (let [[collection id] (feature-key feature)
          [root-col root-id] (root-fn collection id)
          path (path-fn collection id)
          batched-new (batched new-current-batch new-current-batch-size flush-fn)
          cache-batched-new (with-cache feature-cache batched-new cache-store-key cache-value)
          cache-batched-update (batched-updater this new-current-batch new-current-batch-size
                                          delete-current-batch delete-current-batch-size
                                          flush-fn feature-cache)
          batched-history (batched new-history-batch new-history-batch-size flush-fn)
          cached-get-current (use-cache feature-cache cache-use-key)
          batched-append-changelog (batched changelog-batch changelog-batch-size flush-fn)]
      (if-let [current (cached-get-current root-col root-id)]
        (when (util/uuid> (:version feature) (:_version current))
          (let [new-current (merge current path feature)]
            (if (= (:action feature) :close)
              (do (cache-batched-update (sync-valid-to new-current feature))
                  (batched-append-changelog [(:_collection new-current)
                                               (:_id new-current) (:_version current)
                                               (:_version new-current) (:validity feature) :close]))
              (if (t/before? (:_valid_from current) (:validity feature))
                (do ;(println "NOT-SAME")
                  (batched-history (sync-valid-to current feature))
                  ;; reset valid-to for new-current.
                  (cache-batched-update (reset-valid-to (sync-valid-from new-current feature)))
                  (batched-append-changelog [(:_collection new-current)
                                             (:_id new-current) (:_version current)
                                             (:_version new-current) (:validity feature) :change]))
                (do ;(println "SAME")
                  ;; reset valid-to because it might be closed because of nested features.
                  (cache-batched-update (reset-valid-to (sync-valid-from new-current feature)))
                  (batched-append-changelog [(:_collection new-current)
                                             (:_id new-current) (:_version current)
                                             (:_version new-current) (:validity feature) :change]))))))
        (let [nw (sync-valid-from (merge (init-root root-col root-id) path feature) feature)]
          (cache-batched-new nw)
          (batched-append-changelog [(:_collection nw)
                                     (:_id nw) nil
                                     (:_version nw) (:validity feature) :new])))))
  (proj/change-feature [this feature]
    ;; change can be the same, because a new nested feature validity change will also result in a new validity
    ;(println "CHANGE")
    (proj/new-feature this feature))
  (proj/close-feature [this feature]
    (proj/new-feature this feature))
  (proj/delete-feature [this feature]
    (let [[collection id] (feature-key feature)
          [root-col root-id] (root-fn collection id)
          cache-batched-delete (batched-deleter db delete-current-batch delete-current-batch-size
                                                delete-history-batch delete-history-batch-size
                                                flush-fn feature-cache)
          cached-get-current (use-cache feature-cache cache-use-key)
          batched-append-changelog (batched changelog-batch changelog-batch-size flush-fn)]
      (when-let [current (cached-get-current root-col root-id)]
        (when (util/uuid> (:version feature) (:_version current))
          (cache-batched-delete current)
          (doseq [v (:_all_versions current)]
            (batched-append-changelog [(:_collection current)
                                       (:_id current) nil
                                       v (:validity current) :delete]))))))
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

(defn process-chunk* [config dataset chunk]
  (let [cache (create-cache (:db-config config) (:persistence config) dataset chunk)
        timeline (proj/init (create config cache dataset) dataset)]
    (doseq [f chunk]
      (condp = (:action f)
        :new (proj/new-feature timeline f)
        :change (proj/change-feature timeline f)
        :close (proj/close-feature timeline f)
        :delete (proj/delete-feature timeline f)))
    (proj/close timeline)))

(defn process-chunk [{:keys [config chunk dataset]}]
  (with-bench t (log/debug "Processed chunk in" t "ms")
    (flush-batch chunk (partial process-chunk* config dataset))))

(defrecord ChunkedTimeline [config dataset db chunk make-process-fn process-fn]
  proj/Projector
  (proj/init [this for-dataset]
    (let [inited (assoc this :dataset for-dataset)
          inited (assoc inited :process-fn (make-process-fn inited))
          _ (init db for-dataset)]
      inited))
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
  ([config] (create config (volatile! (cache/basic-cache-factory {})) "unknow-dataset"))
  ([config cache for-dataset]
   (let [db (:db-config config)
         persistence (or (:persistence config) (pers/make-cached-jdbc-processor-persistence config))
         new-current-batch-size (or (:new-current-batch-size config) (:batch-size config) 10000)
         new-current-batch (volatile! (clojure.lang.PersistentQueue/EMPTY))
         delete-current-batch-size (or (:delete-current-batch-size config) (:batch-size config) 10000)
         delete-current-batch (volatile! (clojure.lang.PersistentQueue/EMPTY))
         new-history-batch-size (or (:new-history-batch-size config) (:batch-size config) 10000)
         new-history-batch (volatile! (clojure.lang.PersistentQueue/EMPTY))
         delete-history-batch-size (or (:delete-history-batch-size config) (:batch-size config) 10000)
         delete-history-batch (volatile! (clojure.lang.PersistentQueue/EMPTY))
         changelog-batch-size (or (:changelog-batch-size config) (:batch-size config) 10000)
         changelog-batch (volatile! (clojure.lang.PersistentQueue/EMPTY))
         make-flush-fn (make-flush-all new-current-batch delete-current-batch new-history-batch
                              delete-history-batch changelog-batch)]
     (->Timeline for-dataset
      db (partial pers/root persistence) (partial pers/path persistence)
      cache
      new-current-batch new-current-batch-size
      delete-current-batch delete-current-batch-size
      new-history-batch new-history-batch-size
      delete-history-batch delete-history-batch-size
      changelog-batch changelog-batch-size
      make-flush-fn (fn [])))))


(defn create-chunked [config]
  (let [chunk-size (or (:chunk-size config) 10000)
        chunk (volatile! (clojure.lang.PersistentQueue/EMPTY))
        make-process-fn (fn [tl] (batched chunk chunk-size #(process-chunk tl)))
        db (:db-config config)]
    (->ChunkedTimeline config "unknown-dataset" db chunk make-process-fn (fn []))))
