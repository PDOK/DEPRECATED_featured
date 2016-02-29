(ns pdok.featured.extracts
  (:require [clojure.java.jdbc :as j]
            [pdok.featured.config :as config]
            [pdok.featured.json-reader :as json-reader]
            [pdok.featured.mustache  :as m]
            [pdok.featured.timeline :as timeline]
            [pdok.featured.template :as template]
            [pdok.cache :as cache]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a
             :refer [>! <! >!! <!! go chan]])
  (:gen-class))


(def ^{:private true } extract-schema "extractmanagement")
(def ^{:private true } extractset-table "extractmanagement.extractset")
(def ^{:private true } extractset-area-table "extractmanagement.extractset_area")

(defn features-for-extract [dataset feature-type extract-type features]
  "Returns the rendered representation of the collection of features for a given feature-type inclusive tiles-set"
  (if (empty? features)
    [nil nil]
    (let [template-key (template/template-key dataset extract-type feature-type)]
      [nil (map #(vector feature-type (:_version %) (:_tiles %) (m/render template-key %)
                           (:_valid_from %) (:_valid_to %) (:lv-publicatiedatum %)) features)])))


(defn- jdbc-update-extract [db table entries]
  (let [qualified-table (str extract-schema "." table)
        query (str "UPDATE " qualified-table " SET valid_to = ? WHERE version = ?")]
    (try (j/execute! db (cons query entries) :multi? true :transaction? false)
      (catch java.sql.SQLException e (j/print-sql-exception-chain e)))))

(defn update-extract-records [db dataset extract-type items]
  (jdbc-update-extract db (str dataset "_" extract-type "_v0") items))

(defn- jdbc-insert-extract [db table entries]
  (let [qualified-table (str extract-schema "." table)
        query (str "INSERT INTO " qualified-table
                   " (feature_type, version, valid_from, valid_to, publication, tiles, xml) VALUES (?, ?, ?, ?, ?, ?, ?)")]
    (try (j/execute! db (cons query entries) :multi? true :transaction? false)
      (catch java.sql.SQLException e (j/print-sql-exception-chain e)))))

(defn get-or-add-extractset [db extractset version]
  "return id"
  (let [query (str "select id from " extractset-table " where extractset_name = ? and version = ?")]
    (j/with-db-connection [c db]
      (let [result (j/query c [query extractset version])]
        (if (empty? result)
          (do
            (j/query c [(str "SELECT " extract-schema ".add_extractset(?,?)") extractset (int version)])
            (get-or-add-extractset db extractset version))

          (:id (first result)))))))

(defn add-extractset-area [db extractset-id tiles]
  (let [query (str "select * from " extractset-area-table " where extractset_id = ? and area_id = ?")]
    (j/with-db-transaction [c db]
      (doseq [area-id tiles]
        (if (empty? (j/query c [query extractset-id area-id]))
          (j/insert! db extractset-area-table {:extractset_id extractset-id :area_id area-id}))))))

(defn- tiles-from-feature [[type version tiles & more]]
   tiles)

(defn add-metadata-extract-records [db extractset-id rendered-features]
   (let [tiles (reduce clojure.set/union (map tiles-from-feature rendered-features))]
     (add-extractset-area db extractset-id tiles)))

(defn- tranform-feature-for-db [[feature-type version tiles xml-feature valid-from valid-to publication-date]]
  [feature-type version valid-from valid-to publication-date (vec tiles) xml-feature])

(defn add-extract-records [db dataset extract-type rendered-features]
  "Inserts the xml-features and tile-set in an extract schema based on dataset, extract-type, version and feature-type,
   if schema or table doesn't exists it will be created."
  (let [extractset (str dataset "_" extract-type)
        extractset-id (get-or-add-extractset db extractset 0) ]
    (do
      (jdbc-insert-extract db (str extractset "_v0") (map tranform-feature-for-db rendered-features))
      (add-metadata-extract-records db extractset-id rendered-features))
    (count rendered-features)))


(defn transform-and-add-extract [extracts-db dataset feature-type extract-type features]
    (let [[error features-for-extract] (features-for-extract dataset
                                                             feature-type
                                                             extract-type
                                                             features)]
    (if (nil? error)
      (if (nil? features-for-extract)
        {:status "ok" :count 0}
        {:status "ok" :count (add-extract-records extracts-db dataset extract-type features-for-extract)})
      {:status "error" :msg error :count 0})))


(defn- jdbc-delete-versions [db table versions]
  (when (seq versions)
    (let [query (str "DELETE FROM " extract-schema "." table
                     " WHERE version = ?")]
      (try (j/execute! db (cons query (map vector versions)) :multi? true :transaction? false)
           (catch java.sql.SQLException e (j/print-sql-exception-chain e))))))

(defn- delete-extracts-with-version [db dataset feature-type extract-type versions]
  (let [table (str dataset "_" extract-type "_v0_" feature-type)]
    (jdbc-delete-versions db table versions)))

(defn flush-changelog [dataset]
  (do
   (log/info "Delete changelog request voor dataset: " dataset)
   (timeline/delete-changelog (config/timeline-for-dataset dataset))))

(defn file-to-features [path dataset]
  "Helper function to read features from a file.
   Returns features read from file."
  (with-open [s (clojure.java.io/reader path)]
   (doall (json-reader/features-from-stream s :dataset dataset))))

(defn- change-record [record]
  (let [version (:old_version record)
        valid-to (:valid_from record)]
    (when-not (or (nil? version) (nil? valid-to))
      {:version version
       :valid-to valid-to})))

(defn changelog->updates [record]
  (condp = (:action record)
      :change (change-record record)
      nil))

(defn changelog->change-inserts [record]
  (condp = (:action record)
    :new (:feature record)
    :change (:feature record) 
    :close (:feature record)
    nil))

(defn changelog->deletes [record]
  (condp = (:action record)
    :delete (:version record)
    :close (:old_version record)
    nil))

(def ^:dynamic *process-update-extract* (partial update-extract-records config/extracts-db))
(def ^:dynamic *process-insert-extract* (partial transform-and-add-extract config/extracts-db))
(def ^:dynamic *process-delete-extract* (partial delete-extracts-with-version config/extracts-db))
(def ^:dynamic *initialized-collection?* m/registered?)

(defn- create-extract* [dataset extract-type collection fn-timeline-query]
  (let [batch-size 10000
        tl (config/timeline-for-dataset dataset)
        rc (fn-timeline-query tl collection)
        parts (a/pipe rc (a/chan 1 (partition-all batch-size)))]
    (log/info "Start create extracts" (str dataset "-" collection "-" extract-type))
    (loop [i 1
           records (a/<!! parts)]
      (when records
        (*process-insert-extract* dataset collection extract-type
                                  (filter (complement nil?) (map changelog->change-inserts records)))
        (*process-update-extract* dataset collection extract-type 
                                  (filter (complement nil?) (map changelog->updates records)))
        (*process-delete-extract* dataset collection extract-type 
                                  (filter (complement nil?) (map changelog->deletes records)))
        (if (= 0 (mod i 10))
          (log/info "Creating extracts" (str dataset "-" collection "-" extract-type) "processed:" (* i batch-size)))
        (recur (inc i) (a/<!! parts))))))

(defn- create-extract [dataset extract-type fn-timeline-query collections]
   (if-not (every? *initialized-collection?* (map (partial template/template-key dataset extract-type)
                                       collections))
      {:status "error" :msg "missing template(s)" :collections collections}
      (do 
        (doseq [collection collections]
          (create-extract* dataset extract-type collection fn-timeline-query))
        {:status "ok" :collections collections})))

(defn fill-extract [dataset extract-type & more]
  (let [collection (first more)
        tl (config/timeline-for-dataset dataset)]
    (if (nil? collection)
      (create-extract dataset extract-type timeline/changed-features (timeline/collections-in-changelog tl))
      (create-extract dataset extract-type timeline/all-features (list collection)))))

(defn -main [template-location dataset extract-type & more]
  (let [templates-with-metadata (template/templates-with-metadata dataset template-location)]
    (if-not (some false? (map template/add-or-update-template templates-with-metadata))
      (println (apply fill-extract dataset extract-type more))
      (println "could not load template(s)"))))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
