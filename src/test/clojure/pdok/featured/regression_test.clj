(ns ^:regression pdok.featured.regression-test
  (:require [pdok.featured
             [dynamic-config :as dc]
             [config :as config]
             [persistence :as persistence]
             [geoserver :as geoserver]
             [timeline :as timeline]
             [processor :as processor]
             [extracts :as e]
             [json-reader :as json-reader]]
            [pdok.postgres :as pg]
            [pdok.transit :as t]
            [clojure.math.combinatorics :as combo]
            [clojure.core.async :as a
             :refer [>! <! >!! <!! go chan]]
            [clojure.java.jdbc :as j]
            [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [pdok.filestore :as fs])
  (:import (java.util UUID)
           (java.io File)))

(def test-db config/processor-db)
(def changelog-dir (io/file (str (System/getProperty "java.io.tmpdir") "/featured-regression")))

(defn sequential-version-generator []
  (let [i (atom 0)]
    (fn []
      (let [result (UUID/fromString (format "0000000-0000-0000-0000-0000%08d" @i))]
        (swap! i inc)
        result))))

(defn sequential-child-id-generator []
  (let [i (atom 0)]
    (fn []
      (let [result (format "%04d" @i)]
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

(defn- inserted-features [extracts dataset extract-type collection features]
  (let [new-extracts (map #(vector (:_version %) (:_valid_from %) (:_valid_to %) %) features)]
    (swap! extracts concat new-extracts)))

(defn- remove-first [pred coll]
  (keep-indexed #(if (not= %1 (.indexOf coll (first (filter pred coll)))) %2) coll))

(defn- remove-extract-record [extracts record-info]
  (let [[version valid-from] record-info]
    (if valid-from
      (remove-first #(and (= version (nth %1 0))
                          (= valid-from (nth %1 1))) extracts)
      (remove #(= version (nth % 0)) extracts))))

(defn- deleted-versions [extracts dataset collection extract-type records]
    (doseq [record records]
      (swap! extracts remove-extract-record record)))

(defn- query [table selector]
  (when (pg/table-exists? test-db "featured_regression_regression-set" table)
    (let [clauses (pg/map->where-clause selector)]
      (j/query test-db [(str "SELECT * FROM \"featured_regression_regression-set\".\"" table "\" "
                             "WHERE 1 = 1 "
                             "AND " clauses)]))))

(defn make-new-record [line]
  (let [[id version json] (str/split line #"," 3)]
    {:action :new
     :id id
     :version version
     :feature (t/from-json json)}))

(defn make-change-record [line]
  (let [[id old-version new-version json] (str/split line #"," 4)]
    {:action :change
     :id id
     :old-version old-version
     :new-version new-version
     :feature (t/from-json json)}))

(defn make-close-record [line]
  (let [[id version json] (str/split line #"," 3)]
    {:action :close
     :id id
     :version version
     :feature (t/from-json json)}))

(defn make-delete-record [line]
  (let [[id version] (str/split line #"," 2)]
    {:action :delete
     :id id
     :version version}))

(defn- make-changelog-record [csv-line]
  (let [[action rest] (str/split csv-line #"," 2)]
    (condp = action
      "new" (make-new-record rest)
      "change" (make-change-record rest)
      "close" (make-close-record rest)
      "delete" (make-delete-record rest)
      nil)))

(defn- parse-changelog [in-stream]
  "Returns [dataset collection change-record]"
  (let [lines (drop 1 (line-seq (io/reader in-stream)))
        [dataset collection] (str/split (first lines) #",")
        ;drop collection info
        lines (drop 1 lines)]
    [dataset collection (doall (map make-changelog-record lines))]))

(defn- update-db-changelog-counts [changelog-counts]
  (let [records (j/query test-db ["SELECT * FROM \"featured_regression_regression-set\".timeline_changelog"])]
    (doseq [action [:new :change :close :delete]]
      (let [n (count (filter #(= (:action %1) (name action)) records)) ]
        (swap! changelog-counts update action (fnil + 0) n)))

    (doseq [[column stat-name] [[:collection :nil-collections]
                                [:feature_id :nil-ids]]]
      (let [nils (count (filter #(= (get %1 column) nil) records)) ]
        (swap! changelog-counts update stat-name (fnil + 0) nils)))))


(defn process-feature-permutation [meta feature-permutation]
  (println "  " (map #(into [] (map :action %1)) feature-permutation))
  (let [extracts (atom [])
        db-changelog-counts (atom{})]
    (with-bindings
      {#'e/*process-insert-extract* (partial inserted-features extracts)
       #'e/*process-delete-extract* (partial deleted-versions extracts)
       #'e/*initialized-collection?* (constantly true)
       #'processor/*next-version* (sequential-version-generator)
       #'processor/*child-id* (sequential-child-id-generator)}
      (merge (apply merge-with (fn [a b] (if (set? a) (merge a b) (if (sequential? a) (concat a b) (+ a b))))
                    (map (fn [features]
                           (let [persistence (config/persistence)
                                 filestore (config/filestore changelog-dir)
                                 timeline (config/timeline persistence filestore)
                                 projectors (conj (config/projectors persistence) timeline)
                                 processor (processor/create meta "regression-set" persistence projectors)]
                             (dorun (processor/consume processor features))
                             (update-db-changelog-counts db-changelog-counts)
                             (e/fill-extract "regression-set" nil nil)
                             (e/flush-changelog "regression-set")
                             (:statistics (processor/shutdown processor))))
                         feature-permutation))
             {:extracts         extracts
              :db-changelog-counts db-changelog-counts}))))

(defn replay []
  (println "  Replaying")
  (let [persistence (config/persistence)
        filestore (config/filestore changelog-dir)
        projectors (conj (config/projectors persistence) (config/timeline persistence filestore))
        processor (processor/create {} "regression-set" persistence projectors)]
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
        #'dc/*timeline-schema-prefix* :featured_regression
        #'timeline/*changelog-name*      (fn [_# _#] (str ~(clojure.core/name name) "-" (timeline/unique-epoch) ".changelog"))}
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

(defn- test-timeline-db-changelog [expected-counts changelog-counts]
  (is (= 0 (:nil-collections @changelog-counts)))
  (is (= 0 (:nil-ids @changelog-counts)))
  (doseq [action [:new :change :close :delete]]
    (is (= (or ((keyword (str "n-" (name action))) expected-counts) 0)
           (action @changelog-counts)))))

(defn- test-timeline-changelog [{:keys [expected-changelog changelogs]}]
  (let [[expected-dataset expected-collection expected-records]
        (with-open [in (io/reader expected-changelog)] (parse-changelog in))
        filestore (config/filestore changelog-dir)
        parsed-changelogs (doall (map (fn [log] (with-open [in (io/input-stream (fs/get-file filestore log))]
                                                  (parse-changelog in))) changelogs))
        all-records (mapcat (fn [[_ _ records]] records) parsed-changelogs)]
    (is (every? #(= [expected-dataset expected-collection] %) (->> parsed-changelogs (map (partial take 2)))))
    (is (every? #(not (nil? %)) (->> all-records (filter #(#{:new :change :close} (:action %))) (map :feature))))
    (is (every? #(nil? %) (->> all-records (filter #(= :new (:action %))) (map (comp :_valid_to :feature)))))
    (is (every? #(nil? %) (->> all-records (filter #(= :change (:action %))) (map (comp :_valid_to :feature)))))
    (is (every? #(not (nil? %)) (->> all-records (filter #(= :close (:action %))) (map (comp :_valid_to :feature)))))
    (is (= (count expected-records) (count all-records)))
    (doseq [[expected created] (map (fn [e c] [e c]) expected-records all-records)]
      (is (= expected created)))))

(defn- test-timeline
  ([expected-counts results] (test-timeline "col-1" "id-a" expected-counts results))
  ([collection feature-id {:keys [timeline timeline-db-changelog]} results]
   (testing "!>>> Timeline"
     (is (= (:n timeline) (count (query (str "timeline_" collection) {:feature_id feature-id}))))
     (test-timeline-db-changelog timeline-db-changelog (:db-changelog-counts results))
     (test-timeline-changelog results)
     )))

(defn- test-timeline->extract [expected extracts]
  (is (= (:n-extracts expected) (count @extracts)))
  (is (= (:n-extracts expected) (count (distinct (map first @extracts))))) ; extracts should have distinct versions
  (is (= (:n-valid-to expected) (count (filter #((complement nil?) (nth % 2)) @extracts)))))

(defn- query-geoserver [table]
  (if-not (pg/table-exists? test-db "regression-set" table)
    []
    (j/query test-db [ (str "SELECT * FROM \"regression-set\".\"" table "\"" )])))

(defn- test-geoserver
  ([n] (test-geoserver "col-1" n))
  ([collection n]
  (is (= n (count (query-geoserver collection))))))


(defpermutatedtest new results
  (is (= 1 (:n-processed results)))
  (is (= 0 (:n-errored results)))
  (test-persistence {:events 1 :features 1})
  (test-timeline {:timeline              {:n 1}
                  :timeline-db-changelog {:n-new 1}}
                 results)
  (test-timeline->extract {:n-extracts 1
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 1)
  )

(defpermutatedtest new-with-child results
                   (is (= 2 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 1 :features 1})
                   (test-persistence "col-1-child" {:events 1 :features 1})
                   (test-timeline {:timeline              {:n 1}
                                   :timeline-db-changelog {:n-new 1 :n-change 1}}
                                  results)
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}
                                           (:extracts results))
                   (test-geoserver 1)
                   (test-geoserver "col-1-child" 1)
                   )

(defpermutatedtest new-change results
  (is (= 2 (:n-processed results)))
  (is (= 0 (:n-errored results)))
  (test-persistence {:events 2 :features 1})
  (test-timeline {:timeline              {:n 2}
                  :timeline-db-changelog {:n-new 2 :n-close 1}}
                  results)
  (test-timeline->extract {:n-extracts 2
                           :n-valid-to 1}
                          (:extracts results))
  (test-geoserver 1))

(defpermutatedtest new-delete results
  (is (= 2 (:n-processed results)))
  (is (= 0 (:n-errored results)))
  (test-persistence {:events 2 :features 1})
  (test-timeline {:timeline              {:n 0}
                  :timeline-db-changelog {:n-new 1 :n-delete 1}}
                 results)
  (test-timeline->extract {:n-extracts 0
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 0))

(defpermutatedtest new-change-change results
  (is (= 3 (:n-processed results)))
  (is (= 0 (:n-errored results)))
  (test-persistence {:events 3 :features 1})
  (test-timeline {:timeline              {:n 3}
                  :timeline-db-changelog {:n-new 3 :n-close 2}}
                  results)
  (test-timeline->extract {:n-extracts 3
                           :n-valid-to 2}
                          (:extracts results))
  (test-geoserver 1))

(defpermutatedtest new-change-close results
  (is (= 3 (:n-processed results)))
  (is (= 0 (:n-errored results)))
  (test-persistence {:events 3 :features 1})
  (test-timeline {:timeline              {:n 2}
                  :timeline-db-changelog {:n-new 2 :n-close 2}}
                 results)
  (test-timeline->extract {:n-extracts 2
                           :n-valid-to 2}
                          (:extracts results))
  (test-geoserver 0))

(defpermutatedtest new-change-close_with_attributes results
  (is (= 4 (:n-processed results)))
  (is (= 0 (:n-errored results)))
  (test-persistence {:events 4 :features 1})
  (test-timeline {:timeline              {:n 3}
                  :timeline-db-changelog {:n-new 3 :n-close 3}}
                 results)
  (test-timeline->extract {:n-extracts 3
                           :n-valid-to 3}
                          (:extracts results))
  (test-geoserver 0))

(defpermutatedtest new-change-change-delete results
  (is (= 4 (:n-processed results)))
  (is (= 0 (:n-errored results)))
  (test-persistence {:events 4 :features 1})
  (test-timeline {:timeline              {:n 0}
                  :timeline-db-changelog {:n-new 3 :n-close 2 :n-delete 3}}
                 results)
  (test-timeline->extract {:n-extracts 0
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 0))

(defpermutatedtest new-change-change-delete-new-change results
  (is (= 6 (:n-processed results)))
  (is (= 0 (:n-errored results)))
  (test-persistence  {:events 6 :features 1})
  (test-timeline  {:timeline              {:n 2}
                   :timeline-db-changelog {:n-new 5 :n-close 3 :n-delete 3}}
                 results)
  (test-timeline->extract {:n-extracts 2
                           :n-valid-to 1}
                          (:extracts results))
  (test-geoserver 1))

(defpermutatedtest new_with_nested_null_geom results
  (is (= 2 (:n-processed results)))
  (is (= 0 (:n-errored results)))
  (test-persistence  {:events 1 :features 1})
  (test-persistence "col-1$nested" {:events 1 :features 1})
  (test-timeline {:timeline              {:n 1}
                  :timeline-db-changelog {:n-new 1 :n-change 1}}
                 results)
  (test-timeline->extract {:n-extracts 1
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 1))

(defpermutatedtest new_with_nested_crappy_geom results
  (is (= 2 (:n-processed results)))
  (is (= 0 (:n-errored results)))
  (test-persistence {:events 1 :features 1})
  (test-persistence "col-1$nested" {:events 1 :features 1})
  (test-timeline {:timeline              {:n 1}
                  :timeline-db-changelog {:n-new 1 :n-change 1}}
                 results)
  (test-timeline->extract {:n-extracts 1
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 1))


(defpermutatedtest new_nested-change_nested-change_nested results
  (is (= 8 (:n-processed results)))
  (is (= 0 (:n-errored results)))
  (test-persistence {:events 3 :features 1})
  (test-persistence "col-1$nested" {:events 5 :features 3})
  (test-timeline {:timeline              {:n 3}
                  :timeline-db-changelog {:n-new 3 :n-change 3 :n-close 4}}
                 results)
  (test-timeline->extract {:n-extracts 3
                           :n-valid-to 2}
                          (:extracts results))
  (test-geoserver "col-1" 1)
  (test-geoserver "col-1$nested" 1))

(defpermutatedtest new_double_nested-delete-new_double_nested-change_double_nested results
  (is (= (+ 3 3 3 5) (:n-processed results)))
  (is (= 0 (:n-errored results)))
  (test-persistence {:events 4 :features 1})
  (test-persistence "col-1$nestedserie" {:events 5 :features 3})
  (test-persistence "col-1$nestedserie$label" {:events 5 :features 3})
  (test-timeline {:timeline              {:n 2}
                  :timeline-db-changelog {:n-new 3 :n-change 6 :n-delete 3 :n-close 3}}
                 results)
  (test-timeline->extract {:n-extracts 2
                           :n-valid-to 1}
                          (:extracts results))
  (test-geoserver 1)
  (test-geoserver "col-1$nestedserie$label" 1))

(defpermutatedtest new_invalid_nested results
  (is (= 2 (:n-processed results)))
  (is (= 2 (:n-errored results)))
  (test-persistence {:events 0 :features 0})
  (test-persistence "col-1$nested" {:events 0 :features 0})
  (test-timeline {:timeline              {:n 0}
                  :timeline-db-changelog {}}
                 results)
  (test-timeline->extract {:n-extracts 0
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 0)
  (test-geoserver "col-1$nested" 0))

(defpermutatedtest new_double_nested-change_invalid results
  (is (= (+ 3 1 2 2) (:n-processed results)))
  (is (= 5 (:n-errored results)))
  (test-persistence {:events 1 :features 1})
  (test-persistence "col-1$nestedserie" {:events 1 :features 1})
  (test-persistence "col-1$nestedserie$label" {:events 1 :features 1})
  (test-timeline {:timeline              {:n 1}
                  :timeline-db-changelog {:n-new 1 :n-change 2}}
                 results)
  (test-timeline->extract {:n-extracts 1
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 1)
  (test-geoserver "col-1$nestedserie" 0)
  (test-geoserver "col-1$nestedserie$label" 1))

(defregressiontest new-change-change-pand-test results
  (is (= 19 (:n-processed results)))
  (test-persistence "pand" "id-a" {:events 3 :features 1})
  (test-persistence "pand$nummeraanduidingreeks" {:events 9 :features 6})
  (test-persistence "pand$nummeraanduidingreeks$positie" {:events 7 :features 5})
  (test-timeline "pand" "id-a"
                 {:timeline              {:n 3}
                  :timeline-db-changelog {:n-new 3 :n-change 11 :n-close 7}}
                 results)
  (test-timeline->extract {:n-extracts 3
                           :n-valid-to 2}
                          (:extracts results))
  (test-geoserver "pand" 1)
  (test-geoserver "pand$nummeraanduidingreeks" 0)
  (test-geoserver "pand$nummeraanduidingreeks$positie" 3))

(defpermutatedtest same-double-new results
                   (is (= 2 (:n-processed results)))
                   (is (= 1 (:n-errored results)))
                   (test-persistence {:events 1 :features 1})
                   (test-timeline {:timeline              {:n 1}
                                   :timeline-db-changelog {:n-new 1 :n-change 0 :n-close 0}}
                                  results)
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}
                                           (:extracts results))
                   (test-geoserver 1))

(defpermutatedtest new_nested-close_parent results
                   (is (= 4 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 2 :features 1})
                   (test-persistence "col-1$nested" {:events 2 :features 1})
                   (test-timeline {:timeline              {:n 1}
                                   :timeline-db-changelog {:n-new 1 :n-change 1 :n-close 2}}
                                  results)
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 1}
                                           (:extracts results))
                   (test-geoserver "col-1" 0)
                   (test-geoserver "col-1$nested" 0))

(defpermutatedtest new_or_change results
                   (is (= 3 (:n-processed results)))
                   (is (= 0 (:n-errored results)))
                   (test-persistence {:events 3 :features 1})
                   (test-timeline {:timeline              {:n 3}
                                   :timeline-db-changelog {:n-new 3 :n-close 2}}
                                  results)
                   (test-timeline->extract {:n-extracts 3
                                            :n-valid-to 2}
                                           (:extracts results))
                   (test-geoserver 1))