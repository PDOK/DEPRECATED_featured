(ns ^:regression pdok.featured.regression-test
  (:require [pdok.featured
             [dynamic-config :as dc]
             [config :as config]
             [processor :as processor]
             [json-reader :as json-reader]]
            [pdok.postgres :as pg]
            [pdok.transit :as t]
            [clojure.math.combinatorics :as combo]
            [clojure.core.async :refer [>! <! >!! <!! go chan]]
            [clojure.java.jdbc :as j]
            [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [pdok.filestore :as fs])
  (:import (java.io File)
           (java.util UUID)
           (java.util.zip ZipInputStream)))

(def test-db config/processor-db)
(def changelog-dir (io/file (str (System/getProperty "java.io.tmpdir") "/featured-regression")))

(defn sequential-version-generator []
  (let [i (atom 0)]
    (fn []
      (let [result (UUID/fromString (format "0000000-0000-0000-0000-0000%08d" @i))]
        (swap! i inc)
        result))))

(defn with-drops [col]
  (drop 1 (reductions (fn [[d t] v] [(+ d t) v]) [0 0] col)))

(defn permutate-features [feature-stream]
  "Returns a sequence of collection with ordered subsets of the feature stream.
(abcd) wil become (((abcd))((abc)(d))((ab)(cd)) ... )"
  (let [features (doall feature-stream)
        singles (repeat (count features) 1)
        permutations (mapcat combo/permutations
                             (map (fn [c] (map #(count %) c)) (combo/partitions singles)))
        with-drops (map with-drops permutations)
        feature-sets (map (fn [p]
                            (map (fn [[d t] f] (take t (drop d f)))
                                 p (repeat features))) with-drops)]
    feature-sets))

(defn json-features [file-name]
  (let [stream (clojure.java.io/resource file-name)
        [meta features] (json-reader/features-from-stream stream)]
    [meta features]))

(defn- query [table selector]
  (when (pg/table-exists? test-db "featured_regression_regression-set" table)
    (let [clauses (pg/map->where-clause selector)]
      (j/query test-db [(str "SELECT * FROM \"featured_regression_regression-set\".\"" table "\" "
                             "WHERE 1 = 1 "
                             "AND " clauses)]))))

(defn- parse-changelog [in-stream]
  "Returns [dataset collection change-record]"
  (let [lines (drop 1 (line-seq (io/reader in-stream)))
        collection (:collection (t/from-json (first lines)))
        ;drop collection info
        lines (drop 1 lines)]
    [collection (doall (map t/from-json lines))]))

(defn process-feature-permutation [meta feature-permutation]
  (println "  " (map #(into [] (map :action %1)) feature-permutation))
  (let []
    (with-bindings
      {#'processor/*next-version* (sequential-version-generator)}
      (merge (apply merge-with (fn [a b] (if (set? a) (merge a b) (if (sequential? a) (concat a b) (+ a b))))
                    (map (fn [features]
                           (let [persistence (config/persistence)
                                 filestore (config/filestore changelog-dir)
                                 timeline (config/timeline filestore)
                                 processor (processor/create meta "regression-set" persistence [timeline])]
                             (dorun (processor/consume processor features))
                             (:statistics (processor/shutdown processor))))
                         feature-permutation))
             {}))))

(defn replay []
  (println "  Replaying")
  (let [persistence (config/persistence)
        filestore (config/filestore changelog-dir)
        timeline (config/timeline filestore)
        processor (processor/create {} "regression-set" persistence [timeline])]
    (processor/replay processor 1000 nil)
    {:stats (:statistics (processor/shutdown processor)) :extracts nil}))

(defn recursive-delete [^File directory]
  (if (.isDirectory directory)
    (when (reduce #(and %1 (recursive-delete %2)) true (.listFiles directory))
      (.delete directory))
    (.delete directory)))

(defn cleanup []
  (j/execute! test-db ["DROP SCHEMA IF EXISTS \"featured_regression_regression-set\" CASCADE"])
  (j/execute! test-db ["DROP SCHEMA IF EXISTS \"regression-set\" CASCADE"])
  (recursive-delete changelog-dir))

(defmethod  clojure.test/report :begin-test-var [m]
  (with-test-out
    (println ">>"(-> m :var meta :name))))

(defmacro defregressiontest* [name permutated? file results-var & body]
  `(deftest ~name
     (with-bindings
       {#'dc/*persistence-schema-prefix* :featured_regression
        #'dc/*timeline-schema-prefix*    :featured_regression}
       (let [[meta# features#] (json-features (str "regression/" ~file ".json"))
             expected-changelog# (io/resource (str "regression/" ~file ".changelog"))
             permutated# (if ~permutated? (permutate-features features#) [[features#]])
             n# (count permutated#)
             i# (atom 0)]
         (doseq [p# permutated#]
           (swap! i# inc)
           (cleanup)
           (let [~results-var (process-feature-permutation meta# p#)
                 ~results-var (assoc ~results-var :expected-changelog expected-changelog#)]
             ~@body
             (when (= @i# n#)
               (replay)
               ~@body)
             ))))))

(defmacro defpermutatedtest [test-name results-var & body]
  `(defregressiontest* ~test-name true ~(name test-name) ~results-var ~@body))

(defmacro defregressiontest [test-name results-var & body]
  `(defregressiontest* ~test-name false ~(name test-name) ~results-var ~@body))


(defn- test-persistence
  ([events-and-features] (test-persistence "col-1" "id-a" events-and-features))
  ([collection selectors] (test-persistence collection nil selectors))
  ([collection feature-id {:keys [events features]}]
   (testing "!>>> Persistence"
     (is (= events (count (query "feature_stream" (cond-> {:collection collection}
                                                    feature-id (assoc :feature_id feature-id))))))
     (is (= features (count (query "feature" (cond-> {:collection collection}
                                               feature-id (assoc :feature_id feature-id)))))))))

(defn- test-timeline-changelog [{:keys [expected-changelog changelogs]}]
  (let [[expected-collection expected-records]
        (with-open [in (io/reader expected-changelog)] (parse-changelog in))
        filestore (config/filestore changelog-dir)
        parsed-changelogs (doall (map (fn [log] (with-open [in (io/input-stream (fs/get-file filestore log))
                                                            zip (ZipInputStream. in)]
                                                  (.getNextEntry zip)
                                                  (parse-changelog zip))) changelogs))
        all-records (mapcat (fn [[_ records]] records) parsed-changelogs)]
    (is (every? #(= expected-collection %) (->> parsed-changelogs (map first))))
    (is (every? #(not (nil? %)) (->> all-records (filter #(#{"new" "change" "close"} (:action %))) (map :attributes))))
    (is (every? #(nil? %) (->> all-records (filter #(= "new" (:action %))) (map :valid-to))))
    (is (every? #(nil? %) (->> all-records (filter #(= "change" (:action %))) (map :valid-to))))
    (is (every? #(not (nil? %)) (->> all-records (filter #(= "close" (:action %))) (map :valid-to))))
    (is (= (count expected-records) (count all-records)))
    (doseq [[expected created] (map (fn [e c] [e c]) expected-records all-records)]
      (is (= expected created)))))

(defn- test-timeline
  ([expected-counts results] (test-timeline "col-1" "id-a" expected-counts results))
  ([collection feature-id {:keys [timeline _]} results]
   (testing "!>>> Timeline"
     (is (= (:n timeline) (count (query (str "timeline_" collection) {:feature_id feature-id}))))
     (test-timeline-changelog results)
     )))


(defpermutatedtest new results
                   (is (= 1 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 1 :features 1})
                   (test-timeline {:timeline {:n 1}} results))

(defpermutatedtest new-with-child-geometry results
                   (is (= 1 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 1 :features 1})
                   (test-timeline {:timeline {:n 1}} results))

(defpermutatedtest new-with-child-geometry-in-array results
                   (is (= 1 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 1 :features 1})
                   (test-timeline {:timeline {:n 1}} results))

;; Won't work anymore: adds child separately.
;(defpermutatedtest new-with-child results
;                   (is (= 2 (:n-processed results)))
;                   (is (= 0 (:n-errored results)))
;                   (test-persistence {:events 1 :features 1})
;                   (test-timeline {:timeline {:n 1}} results))

(defpermutatedtest new-change results
                   (is (= 2 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 2 :features 1})
                   (test-timeline {:timeline {:n 2}} results))

(defpermutatedtest new-delete results
                   (is (= 2 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 2 :features 1})
                   (test-timeline {:timeline {:n 0}} results))

(defpermutatedtest new-change-change results
                   (is (= 3 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 3 :features 1})
                   (test-timeline {:timeline {:n 3}} results))

(defpermutatedtest new-change-close results
                   (is (= 3 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 3 :features 1})
                   (test-timeline {:timeline {:n 2}} results))

(defpermutatedtest new-change-close_with_attributes results
                   (is (= 4 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 4 :features 1})
                   (test-timeline {:timeline {:n 2}} results))

(defpermutatedtest new-change-change-delete results
                   (is (= 4 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 4 :features 1})
                   (test-timeline {:timeline {:n 0}} results))

(defpermutatedtest new-change-change-delete-new-change results
                   (is (= 6 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 6 :features 1})
                   (test-timeline {:timeline {:n 2}} results))

(defpermutatedtest new_with_nested_null_geom results
                   (is (= 1 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 1 :features 1})
                   (test-timeline {:timeline {:n 1}} results))

(defpermutatedtest new_with_nested_crappy_geom results
                   (is (= 1 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 1 :features 1})
                   (test-timeline {:timeline {:n 1}} results))

(defpermutatedtest new_nested-change_nested-change_nested results
                   (is (= 3 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 3 :features 1})
                   (test-timeline {:timeline {:n 3}} results))

(defpermutatedtest new_double_nested-delete-new_double_nested-change_double_nested results
                   (is (= 4 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 4 :features 1})
                   (test-timeline {:timeline {:n 2}} results))

(defpermutatedtest new_invalid_nested results
                   (is (= 1 (:n-processed results)))
                   (is (= 1 (:n-errored results)))
                   (test-persistence {:events 0 :features 0})
                   (test-timeline {:timeline {:n 0}} results))

(defpermutatedtest new_double_nested-change_invalid results
                   (is (= 2 (:n-processed results)))
                   (is (= 1 (:n-errored results)))
                   (test-persistence {:events 1 :features 1})
                   (test-timeline {:timeline {:n 1}} results))

(defregressiontest new-change-change-pand-test results
                   (is (= 3 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence "pand" "id-a" {:events 3 :features 1})
                   (test-timeline "pand" "id-a" {:timeline {:n 3}} results))

(defpermutatedtest same-double-new results
                   (is (= 2 (:n-processed results)))
                   (is (= 1 (:n-errored results)))
                   (test-persistence {:events 1 :features 1})
                   (test-timeline {:timeline {:n 1}} results))

(defpermutatedtest new_nested-close_parent results
                   (is (= 2 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 2 :features 1})
                   (test-timeline {:timeline {:n 1}} results))

(defpermutatedtest new_or_change results
                   (is (= 3 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 3 :features 1})
                   (test-timeline {:timeline {:n 3}} results))

(defregressiontest new-change_with_array_attribute results
                   (is (= 2 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 2 :features 1})
                   (test-timeline {:timeline {:n 2}} results))
