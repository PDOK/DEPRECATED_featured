(ns pdok.featured.extracts
  (:require [clojure.java.jdbc :as j]
            [pdok.featured.config :as config]
            [pdok.featured.json-reader :as json-reader]
            [pdok.featured.mustache :as m]
            [pdok.featured.timeline :as timeline]
            [pdok.featured.template :as template]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a
             :refer [>! <! >!! <!! go chan]]
            [clojure.string :as str])
  (:gen-class)
  (:import (java.sql SQLException)))


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

(defn- jdbc-insert-extract [db table entries]
  (when (seq entries)
    (let [qualified-table (str extract-schema "." table)
          query (str "INSERT INTO " qualified-table
                     " (feature_type, version, valid_from, valid_to, publication, tiles, xml) VALUES (?, ?, ?, ?, ?, ?, ?)")]
      (try (j/execute! db (cons query entries) :multi? true :transaction? (:transaction? db))
           (catch SQLException e
             (log/with-logs ['pdok.featured.extracts :error :error] (j/print-sql-exception-chain e))
             (throw e))))))

(defn get-or-add-extractset [db dataset extract-type]
  "return id"
  (let [query (str "select id, name from " extractset-table " where name = ? and extract_type = ?")]
    (j/with-db-connection [c db]
      (let [result (j/query c [query dataset extract-type])]
        (if (empty? result)
          (do
            (j/query c [(str "SELECT " extract-schema ".add_extractset(?,?)")
                        dataset
                        extract-type
                        ])
            (get-or-add-extractset db dataset extract-type))

          {:extractset-id (:id (first result))
           :extractset-name (:name (first result))})))))

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
  (let [{:keys [extractset-id extractset-name]} (get-or-add-extractset db dataset extract-type) ]
    (do
      (jdbc-insert-extract db (str extractset-name "_" extract-type) (map tranform-feature-for-db rendered-features))
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
        (let [n-inserted-records (add-extract-records extracts-db dataset extract-type features-for-extract)]
          (log/info "Extract records inserted: " n-inserted-records (str/join "-" (list dataset feature-type extract-type)))
          {:status "ok" :count n-inserted-records}))
      (do
        (log/error "Error creating extracts" error)
        {:status "error" :msg error :count 0}))))


(defn- jdbc-delete-versions [db table versions]
  "([version valid_from][version valid_from] ... )"
  (let [versions-only (map #(take 1 %) (filter (fn [[_ valid-from]] (not valid-from)) versions))
        with-valid-from (filter (fn [[_ valid-from]] valid-from) versions)]
    (when (seq versions-only)
      (let [query (str "DELETE FROM " extract-schema "." table
                       " WHERE version = ?")]
        (try (j/execute! db (cons query versions-only) :multi? true :transaction? (:transaction? db))
             (catch SQLException e
               (log/with-logs ['pdok.featured.extracts :error :error] (j/print-sql-exception-chain e))
               (throw e)))))
    (when (seq with-valid-from)
      (let [query (str "DELETE FROM " extract-schema "." table
                       " WHERE version = ? AND valid_from = ?")]
        (try (j/execute! db (cons query with-valid-from) :multi? true :transaction? (:transaction? db))
             (catch SQLException e
               (log/with-logs ['pdok.featured.extracts :error :error] (j/print-sql-exception-chain e))
               (throw e)))))))

(defn- delete-extracts-with-version [db dataset feature-type extract-type versions]
  (let [table (str dataset "_" extract-type "_" feature-type)]
    (jdbc-delete-versions db table versions)))

(defn flush-changelog [dataset]
  (do
   (log/info "Delete changelog request for dataset: " dataset)
   (timeline/delete-changelog (config/timeline-for-dataset dataset))))

(defn file-to-features [path dataset]
  "Helper function to read features from a file.
   Returns features read from file."
  (with-open [s (clojure.java.io/reader path)]
   (doall (json-reader/features-from-stream s :dataset dataset))))

(defn changelog->change-inserts [record]
  (condp = (:action record)
    :new (:feature record)
    :change (:feature record)
    :close (:feature record)
    nil))

(defn changelog->deletes [record]
  (condp = (:action record)
    :delete [(:version record)]
    :change [(:old_version record) (:valid_from record)]
    :close [(:old_version record) (:valid_from record)]
    nil))

(def ^:dynamic *process-insert-extract* (partial transform-and-add-extract config/extracts-db))
(def ^:dynamic *process-delete-extract* (partial delete-extracts-with-version config/extracts-db))
(def ^:dynamic *initialized-collection?* m/registered?)

(defn- create-extract* [dataset collection extract-type fn-timeline-query]
  (let [batch-size 10000
        tl (config/timeline-for-dataset dataset)
        rc (fn-timeline-query tl collection)
        parts (a/pipe rc (a/chan 1 (partition-all batch-size)))]
    (log/info "Start create extracts" (str dataset "-" extract-type "-" collection))
    (loop [i 1
           records (a/<!! parts)]
      (when records
        (*process-insert-extract* dataset collection extract-type
                                  (filter (complement nil?) (map changelog->change-inserts records)))
        (*process-delete-extract* dataset collection extract-type
                                  (filter (complement nil?) (map changelog->deletes records)))
        (if (= 0 (mod i 10))
          (log/info "Creating extracts" (str dataset "-" extract-type "-" collection) "processed:" (* i batch-size)))
        (recur (inc i) (a/<!! parts))))
    (log/info "Finished extract" (str dataset "-" extract-type "-" collection))))

(defn- create-extract [dataset extract-type fn-timeline-query collections]
   (if-not (every? *initialized-collection?* (map (partial template/template-key dataset extract-type)
                                       collections))
      {:status "error" :msg "missing template(s)" :collections collections}
      (do
        (doseq [collection collections]
          (create-extract* dataset collection extract-type fn-timeline-query))
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
