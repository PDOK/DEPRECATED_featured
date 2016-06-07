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
            [clojure.math.combinatorics :as combo]
            [clojure.core.async :as a
             :refer [>! <! >!! <!! go chan]]
            [clojure.java.jdbc :as j]
            [clojure.test :refer :all]))

(def test-db config/processor-db)

(def test-gml
    "<gml:Point srsName=\"urn:ogc:def:crs:EPSG::28992\" xmlns:gml=\"http://www.opengis.net/gml\"><gmp:pos>172307.599 509279.740</gml:pos></gml:Point>"
 )

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
  (doseq [f features]
    (let [record (vector (:_version f) (:_valid_from f) (:_valid_to f) f)]
      (swap! extracts conj record))))

(defn- remove-extract-record [extracts record-info]
  (let [[version valid-from] record-info]
    (if valid-from
      (into [] (remove #(and (= version  (nth %1 0))
                             (= valid-from (nth %1 1))) extracts))
      (into [] (remove #(= version (nth % 0)) extracts)))))

(defn- deleted-versions [extracts dataset collection extract-type records]
    (doseq [record records]
      (swap! extracts remove-extract-record record)))

(defn- query [table selector]
  (when (pg/table-exists? test-db "featured_regression_regression-set" table)
    (let [clauses (pg/map->where-clause selector)]
      (j/query test-db [(str "SELECT * FROM \"featured_regression_regression-set\".\"" table "\" "
                             "WHERE 1 = 1 "
                             "AND " clauses)]))))

(defn- update-changelog-counts [changelog-counts]
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
        changelog-counts (atom{})]
    (with-bindings
      {#'e/*process-insert-extract* (partial inserted-features extracts)
       #'e/*process-delete-extract* (partial deleted-versions extracts)
       #'e/*initialized-collection?* (constantly true)}
      {:stats (apply merge-with (fn [a b]  (if (set? a) (merge a b) (if (seq? a) (conj a b) (+ a b))))
            (map (fn [features]
                (let [persistence (config/persistence)
                      projectors (conj (config/projectors persistence) (config/timeline persistence))
                      processor (processor/create meta "regression-set" persistence projectors)]
                  (dorun (processor/consume processor features))
                  (update-changelog-counts changelog-counts)
                  (e/fill-extract "regression-set" nil nil)
                  (e/flush-changelog "regression-set")
                  (:statistics (processor/shutdown processor))))
              feature-permutation))
      :extracts extracts
      :changelog-counts changelog-counts})))

(defn replay []
  (println "  Replaying")
  (let [persistence (config/persistence)
        projectors (conj (config/projectors persistence) (config/timeline persistence))
        processor (processor/create {} "regression-set" persistence projectors)]
    (processor/replay processor 1000 nil)
    {:stats (:statistics (processor/shutdown processor)) :extracts nil}))

(defn clean-db []
  (j/execute! test-db ["DROP SCHEMA IF EXISTS \"featured_regression_regression-set\" CASCADE"])
  (j/execute! test-db ["DROP SCHEMA IF EXISTS \"regression-set\" CASCADE"]))

(defmethod  clojure.test/report :begin-test-var [m]
  (with-test-out
    (println ">>"(-> m :var meta :name))))

(defmacro defregressiontest* [name permutated? file results-var & body]
  `(deftest ~name
     (with-bindings
       {#'dc/*persistence-schema-prefix* :featured_regression
        #'dc/*timeline-schema-prefix* :featured_regression}
       (let [[meta# features#] (json-features (str "regression/" ~file ".json"))
             permutated# (if ~permutated? (permutate-features features#) [[features#]])
             n# (count permutated#)
             i# (atom 0)]
         (doseq [p# permutated#]
           (swap! i# inc)
           (clean-db)
           (let [~results-var (process-feature-permutation meta# p#)]
             ~@body
             (when (= @i# n#)
               (replay)
               ~@body)
             ))))))

(defmacro defpermutatedtest [name file results-var & body]
  `(defregressiontest* ~name true ~file ~results-var ~@body))

(defmacro defregressiontest [name file results-var & body]
  `(defregressiontest* ~name false ~file ~results-var ~@body))


(defn- test-persistence
  ([events-and-features] (test-persistence "col-1" "id-a" events-and-features))
  ([collection selectors] (test-persistence collection nil selectors))
  ([collection feature-id {:keys [events features]}]
   (testing "!>>> Persistence"
     (is (= events (count (query "feature_stream" (cond-> {:collection collection}
                                                    feature-id (assoc :feature_id feature-id))))))
     (is (= features (count (query "feature" (cond-> {:collection collection}
                                               feature-id (assoc :feature_id feature-id)))))))))

(defn- test-timeline-changelog [expected-counts changelog-counts]
  (is (= 0 (:nil-collections @changelog-counts)))
  (is (= 0 (:nil-ids @changelog-counts)))
  (doseq [action [:new :change :close :delete]]
    (is (= (or ((keyword (str "n-" (name action))) expected-counts) 0)
           (action @changelog-counts)))))

(defn- test-timeline
  ([expected-counts changelog-counts] (test-timeline "col-1" "id-a" expected-counts changelog-counts))
  ([collection feature-id {:keys [timeline timeline-changelog]} changelog-counts]
   (testing "!>>> Timeline"
     (is (= (:n timeline) (count (query (str "timeline_" collection) {:feature_id feature-id}))))
     (test-timeline-changelog timeline-changelog changelog-counts))))

(defn- test-timeline->extract [expected extracts]
  ;  (println "EXTRACTS\n" (clojure.string/join "\n" (map (fn [e] (into [] (take 2 e))) @extracts)))
  (is (= (:n-extracts expected) (count @extracts)))
  (is (= (:n-valid-to expected) (count (filter #((complement nil?) (nth % 2)) @extracts)))))

(defn- query-geoserver [table]
  (if-not (pg/table-exists? test-db "regression-set" table)
    []
    (j/query test-db [ (str "SELECT * FROM \"regression-set\".\"" table "\"" )])))

(defn- test-geoserver
  ([n] (test-geoserver "col-1" n))
  ([collection n]
  (is (= n (count (query-geoserver collection))))))


(defpermutatedtest new "new" results
  (is (= 1 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence {:events 1 :features 1})
  (test-timeline {:timeline {:n 1}
                  :timeline-changelog {:n-new 1}}
                 (:changelog-counts results))
  (test-timeline->extract {:n-extracts 1
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 1)
  )

(defpermutatedtest new-with-child "new-with-child" results
                   (is (= 2 (:n-processed (:stats results))))
                   (is (= 0 (:n-errored (:stats results))))
                   (test-persistence {:events 1 :features 1})
                   (test-persistence "col-1-child" {:events 1 :features 1})
                   (test-timeline {:timeline {:n 1}
                                   :timeline-changelog {:n-new 1 :n-change 1}}
                                  (:changelog-counts results))
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}
                                           (:extracts results))
                   (test-geoserver 1)
                   (test-geoserver "col-1-child" 1)
                   )

(defpermutatedtest new-change "new-change" results
  (is (= 2 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence {:events 2 :features 1})
  (test-timeline {:timeline {:n 2}
                  :timeline-changelog {:n-new 2 :n-close 1}}
                  (:changelog-counts results))
  (test-timeline->extract {:n-extracts 2
                           :n-valid-to 1}
                          (:extracts results))
  (test-geoserver 1))

(defpermutatedtest new-delete "new-delete" results
  (is (= 2 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence {:events 2 :features 1})
  (test-timeline {:timeline {:n 0}
                  :timeline-changelog {:n-new 1 :n-delete 1}}
                 (:changelog-counts results))
  (test-timeline->extract {:n-extracts 0
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 0))

(defpermutatedtest new-change-change "new-change-change" results
  (is (= 3 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence {:events 3 :features 1})
  (test-timeline {:timeline {:n 3}
                  :timeline-changelog {:n-new 3 :n-close 2}}
                  (:changelog-counts results))
  (test-timeline->extract {:n-extracts 3
                           :n-valid-to 2}
                          (:extracts results))
  (test-geoserver 1))

(defpermutatedtest new-change-close "new-change-close" results
  (is (= 3 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence {:events 3 :features 1})
  (test-timeline {:timeline {:n 2}
                  :timeline-changelog {:n-new 2 :n-close 2}}
                 (:changelog-counts results))
  (test-timeline->extract {:n-extracts 2
                           :n-valid-to 2}
                          (:extracts results))
  (test-geoserver 0))

(defpermutatedtest new-change-close-with-attributes "new-change-close_with_attributes" results
  (is (= 4 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence {:events 4 :features 1})
  (test-timeline {:timeline {:n 3}
                  :timeline-changelog {:n-new 3 :n-close 3}}
                 (:changelog-counts results))
  (test-timeline->extract {:n-extracts 3
                           :n-valid-to 3}
                          (:extracts results))
  (test-geoserver 0))

(defpermutatedtest new-change-change-delete "new-change-change-delete" results
  (is (= 4 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence {:events 4 :features 1})
  (test-timeline {:timeline{:n 0}
                  :timeline-changelog {:n-new 3 :n-close 2 :n-delete 3}}
                 (:changelog-counts results))
  (test-timeline->extract {:n-extracts 0
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 0))

(defpermutatedtest new-change-change-delete-new-change "new-change-change-delete-new-change" results
  (is (= 6 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence  {:events 6 :features 1})
  (test-timeline  {:timeline {:n 2}
                   :timeline-changelog {:n-new 5 :n-close 3 :n-delete 3}}
                 (:changelog-counts results))
  (test-timeline->extract {:n-extracts 2
                           :n-valid-to 1}
                          (:extracts results))
  (test-geoserver 1))

(defpermutatedtest new-with-nested-null-geom "new_with_nested_null_geom" results
  (is (= 2 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence  {:events 1 :features 1})
  (test-persistence "col-1$nested" {:events 1 :features 1})
  (test-timeline {:timeline {:n 1}
                  :timeline-changelog {:n-new 1 :n-change 1}}
                 (:changelog-counts results))
  (test-timeline->extract {:n-extracts 1
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 1))

(defpermutatedtest new-with-nested-crappy-geom "new_with_nested_crappy_geom" results
  (is (= 2 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence {:events 1 :features 1})
  (test-persistence "col-1$nested" {:events 1 :features 1})
  (test-timeline {:timeline {:n 1}
                  :timeline-changelog {:n-new 1 :n-change 1}}
                 (:changelog-counts results))
  (test-timeline->extract {:n-extracts 1
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 1))


(defpermutatedtest new-nested_change-nested_change-nested "new_nested-change_nested-change_nested" results
  (is (= 8 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence {:events 3 :features 1})
  (test-persistence "col-1$nested" {:events 5 :features 3})
  (test-timeline {:timeline {:n 3}
                  :timeline-changelog {:n-new 3 :n-change 3 :n-close 4}}
                 (:changelog-counts results))
  (test-timeline->extract {:n-extracts 3
                           :n-valid-to 2}
                          (:extracts results))
  (test-geoserver 1))

(defpermutatedtest new_double_nested-delete-new_double_nested-change_double_nested "new_double_nested-delete-new_double_nested-change_double_nested" results
  (is (= (+ 3 3 3 5) (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence {:events 4 :features 1})
  (test-persistence "col-1$nestedserie" {:events 5 :features 3})
  (test-persistence "col-1$nestedserie$label" {:events 5 :features 3})
  (test-timeline {:timeline {:n 2}
                  :timeline-changelog {:n-new 3 :n-change 6 :n-delete 3 :n-close 3}}
                 (:changelog-counts results))
  (test-timeline->extract {:n-extracts 2
                           :n-valid-to 1}
                          (:extracts results))
  (test-geoserver 1)
  (test-geoserver "col-1$nestedserie$label" 1))

(defpermutatedtest new_invalid_nested "new_invalid_nested" results
  (is (= 2 (:n-processed (:stats results))))
  (is (= 2 (:n-errored (:stats results))))
  (test-persistence {:events 0 :features 0})
  (test-persistence "col-1$nested" {:events 0 :features 0})
  (test-timeline {:timeline {:n 0}
                  :timeline-changelog {}}
                 (:changelog-counts results))
  (test-timeline->extract {:n-extracts 0
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 0)
  (test-geoserver "col-1$nested" 0))

(defpermutatedtest new_double_nested-change_invalid "new_double_nested-change_invalid" results
  (is (= (+ 3 1 2 2) (:n-processed (:stats results))))
  (is (= 5 (:n-errored (:stats results))))
  (test-persistence {:events 1 :features 1})
  (test-persistence "col-1$nestedserie" {:events 1 :features 1})
  (test-persistence "col-1$nestedserie$label" {:events 1 :features 1})
  (test-timeline {:timeline {:n 1}
                  :timeline-changelog {:n-new 1 :n-change 2}}
                 (:changelog-counts results))
  (test-timeline->extract {:n-extracts 1
                           :n-valid-to 0}
                          (:extracts results))
  (test-geoserver 1)
  (test-geoserver "col-1$nestedserie" 0)
  (test-geoserver "col-1$nestedserie$label" 1))

(defregressiontest pand-new-change-change-test "new-change-change-pand-test" results
  (is (= 19 (:n-processed (:stats results))))
  (test-persistence "pand" "id-a" {:events 3 :features 1})
  (test-persistence "pand$nummeraanduidingreeks" {:events 9 :features 6})
  (test-persistence "pand$nummeraanduidingreeks$positie" {:events 7 :features 5})
  (test-timeline "pand" "id-a"
                 {:timeline {:n 3}
                  :timeline-changelog {:n-new 3 :n-change 11 :n-close 7}}
                 (:changelog-counts results))
  (test-timeline->extract {:n-extracts 3
                           :n-valid-to 2}
                          (:extracts results))
  (test-geoserver "pand" 1)
  (test-geoserver "pand$nummeraanduidingreeks" 0)
  (test-geoserver "pand$nummeraanduidingreeks$positie" 3))

(defpermutatedtest same-double-new "same-double-new" results
                   (is (= 2 (:n-processed (:stats results))))
                   (is (= 1 (:n-errored (:stats results))))
                   (test-persistence {:events 1 :features 1})
                   (test-timeline {:timeline {:n 1}
                                   :timeline-changelog {:n-new 1 :n-change 0 :n-close 0}}
                                  (:changelog-counts results))
                   (test-timeline->extract {:n-extracts 1
                                            :n-valid-to 0}
                                           (:extracts results))
                   (test-geoserver 1))