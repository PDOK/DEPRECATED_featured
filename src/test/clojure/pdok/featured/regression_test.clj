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

(defn- inserted-features [extracts dataset collection extract-type features]
  (doseq [f features]
    (let [max-row-id (if (empty? @extracts)
                       0
                       (reduce max (map first @extracts)))]
          (swap! extracts assoc (inc max-row-id) f))))


(defn- filter-collection [item-to-filter collection] 
  (some #(= % item-to-filter) collection))

(defn- deleted-versions [extracts dataset collection extract-type versions-to-delete]
  (let [rows-to-delete (map first (filter #(filter-collection (:_version (second %)) versions-to-delete) @extracts))]
    (doseq [row-id rows-to-delete]
      (swap! extracts dissoc row-id))))

;(defn- deleted-versions [extracts dataset collection extract-type versions-to-delete]
;  (println "do nothing"))


(defn process-feature-permutation [meta feature-permutation]
  (println "  " (map #(into [] (map :action %1)) feature-permutation))
  (let [extracts (atom {})]
    (with-bindings
      {#'e/*process-insert-extract* (partial inserted-features extracts)
       #'e/*process-delete-extract* (partial deleted-versions extracts)
       #'e/*initialized-collection?* (constantly true)}
      {:stats (apply merge-with (fn [a b] (if (seq? a) (conj a b) (+ a b)))
            (map (fn [features]
                (let [persistence (config/persistence)
                      projectors (conj (config/projectors persistence) (config/timeline persistence))
                      processor (processor/create meta "regression-set" persistence projectors)]
                  (dorun (processor/consume processor features))
                  (e/fill-extract "regression-set" nil)
                  (e/flush-changelog "regression-set")
                  (:statistics (processor/shutdown processor))))
              feature-permutation)) 
      :extracts extracts})))

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
           (let [~results-var (process-feature-permutation meta# p#)
                 _# (println "extracts: " @(:extracts ~results-var))]
             ~@body
             (when (= @i# n#)
               (replay)
               ~@body)))))))

(defmacro defpermutatedtest [name file results-var & body]
  `(defregressiontest* ~name true ~file ~results-var ~@body))

(defmacro defregressiontest [name file results-var & body]
  `(defregressiontest* ~name false ~file ~results-var ~@body))

(defn- query [table selector]
  (let [clauses (pg/map->where-clause selector)]
  (j/query test-db [ (str "SELECT * FROM \"featured_regression_regression-set\"." table " "
                          "WHERE 1 = 1 "
                          "AND " clauses )])))

(defn- test-persistence
  ([collection selectors] (test-persistence collection nil selectors))
  ([collection feature-id {:keys [events features]}]
   (testing "!>>> Persistence"
     (is (= events (count (query "feature_stream" (cond-> {:collection collection}
                                                    feature-id (assoc :feature_id feature-id))))))
     (is (= features (count (query "feature" (cond-> {:collection collection}
                                               feature-id (assoc :feature_id feature-id)))))))))


(defn- test-timeline [collection feature-id {:keys [timeline-current timeline timeline-changelog]}]
  (testing "!>>> Timeline"
    (is (= (:n timeline-current) (count (query "timeline_current" {:collection collection :feature_id feature-id}))))
    (is (= (:n timeline) (count (query "timeline" {:collection collection :feature_id feature-id}))))
    ;(is (= (or (:n-new timeline-changelog) 0)
    ;       (count (query "timeline_changelog" {:collection collection :action "new"}))))
    ;(is (= (or (:n-change timeline-changelog) 0)    
    ;       (count (query "timeline_changelog" {:collection collection :action "change"}))))
    ;(is (= (or (:n-close timeline-changelog) 0)
    ;       (count (query "timeline_changelog" {:collection collection :action "close"}))))
    ;(is (= (or (:n-delete timeline-changelog) 0)
    ;       (count (query "timeline_changelog" {:collection collection :action "delete"}))))
    ))

(defn- test-timeline->extract [extracts n-extracts]
  (is (= n-extracts (count @extracts))))

(defn- query-geoserver [table]
  (if-not (pg/table-exists? test-db "regression-set" table)
    []
    (j/query test-db [ (str "SELECT * FROM \"regression-set\".\"" table "\"" )])))

(defn- test-geoserver [collection n]
  (is (= n (count (query-geoserver collection)))))


(defpermutatedtest new-feature "col-2_id-b_new" results
  (is (= 1 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence "col-2" "id-b" {:events 1 :features 1})
  (test-timeline "col-2" "id-b" {:timeline-current {:n 1}
                                 :timeline {:n 0}
                                 :timeline-changelog {:n-new 1}})
  (test-timeline->extract (:extracts results) 1)
  (test-geoserver "col-2" 1))

(defpermutatedtest new-change-feature "col-2_id-b_new-change" results
  (is (= 2 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence "col-2" "id-b" {:events 2 :features 1})
  (test-timeline "col-2" "id-b" {:timeline-current {:n 1}
                                 :timeline {:n 1}})
  (println "results: " results)
  (println "extracts: " @(:extracts results))
  (test-timeline->extract (:extracts results) 2)
  (test-geoserver "col-2" 1))

(defpermutatedtest new-change-close-feature "col-2_id-b_new-change-close" results
  (is (= 3 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence "col-2" "id-b" {:events 3 :features 1})
  (test-timeline "col-2" "id-b" {:timeline-current {:n 1}
                                 :timeline {:n 1}
                                 :timeline-changelog {:n-new 1 :n-change 1 :n-close 1}})
  (test-timeline->extract (:extracts results) 2)
  (test-geoserver "col-2" 0))

(defpermutatedtest new-change-close_with_attributes-feature "col-2_id-b_new-change-close_with_attributes" results
  (is (= 4 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence "col-2" "id-b" {:events 4 :features 1})
  (test-timeline "col-2" "id-b" {:timeline-current {:n 1}
                                 :timeline {:n 2}
                                 :timeline-changelog {:n-new 1 :n-change 2 :n-close 1}})
  (test-timeline->extract (:extracts results) 3)
  (test-geoserver "col-2" 0))

(defpermutatedtest new-change-change-delete-feature "col-1_id-a_new-change-change-delete" results
  (is (= 4 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence "col-1" "id-a" {:events 4 :features 1})
  (test-timeline "col-1" "id-a" {:timeline-current {:n 0}
                                 :timeline {:n 0}
                                 :timeline-changelog {:n-new 1 :n-change 2 :n-delete 3}})
  (test-timeline->extract (:extracts results) 0)
  (test-geoserver "col-1" 0))

(defpermutatedtest new-change-change-delete-new-change-feature "col-1_id-a_new-change-change-delete-new-change" results
  (is (= 6 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence "col-1" "id-a" {:events 6 :features 1})
  (test-timeline "col-1" "id-a" {:timeline-current {:n 1}
                                 :timeline {:n 1}
                                 :timeline-changelog {:n-new 2 :n-change 3 :n-delete 3}})
  (test-timeline->extract (:extracts results) 2)
  (test-geoserver "col-1" 1))

(defpermutatedtest new-feature-with-nested-null-geom "col-2_id-b_new_with_nested_null_geom" results
  (is (= 2 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence "col-2" "id-b" {:events 1 :features 1})
  (test-persistence "col-2$nested" {:events 1 :features 1})
  (test-timeline "col-2" "id-b" {:timeline-current {:n 1}
                                 :timeline {:n 0}
                                 :timeline-changelog {:n-new 1 :n-change 1}})
  (test-timeline->extract (:extracts results) 1)
  (test-geoserver "col-2" 1))

(defpermutatedtest new-feature-with-nested-crappy-geom "col-2_id-b_new_with_nested_crappy_geom" results
  (is (= 2 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence "col-2" "id-b" {:events 1 :features 1})
  (test-persistence "col-2$nested" {:events 1 :features 1})
  (test-timeline "col-2" "id-b" {:timeline-current {:n 1}
                                 :timeline {:n 0}
                                 :timeline-changelog {:n-new 1 :n-change 1}})
  (test-timeline->extract (:extracts results) 1)
  (test-geoserver "col-2" 1))

(defpermutatedtest new-delete-feature "col-3_id-c_new-delete" results
  (is (= 2 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence "col-3" "id-c" {:events 2 :features 1})
  (test-timeline "col-3" "id-c" {:timeline-current {:n 0}
                                 :timeline {:n 0}})
  (test-timeline->extract (:extracts results) 0)
  (test-geoserver "col-3" 0))

(defpermutatedtest nested-new-change-change "col-3_id-c_nested-new-change-change" results
  (is (= 8 (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence "col-3" "id-c" {:events 3 :features 1})
  (test-persistence "col-3$nested" {:events 5 :features 3})
  (test-timeline "col-3" "id-c" {:timeline-current {:n 1}
                                 :timeline {:n 2}})
  (test-timeline->extract (:extracts results) 3)
  (test-geoserver "col-3" 1))

(defregressiontest double-nested-new-delete-new-change-feature "col-2_id-b_double_nested_new_delete_new_change" results
  (is (= (+ 3 3 3 5) (:n-processed (:stats results))))
  (is (= 0 (:n-errored (:stats results))))
  (test-persistence "col-2" "id-b" {:events 4 :features 1})
  (test-persistence "col-2$nestedserie" {:events 5 :features 3})
  (test-persistence "col-2$nestedserie$label" {:events 5 :features 3})
  (test-timeline "col-2" "id-b" {:timeline-current {:n 1}
                                 :timeline {:n 1}
                                 :timeline-changelog {:n-new 2 :n-change 7 :n-delete 3 :n-close 2}})
  (test-timeline->extract (:extracts results) 2)
  (test-geoserver "col-2" 1)
  (test-geoserver "col-2$nestedserie$label" 1))

(defpermutatedtest invalid-nested-new-feature "col-2_id-b_invalid_nested_new" results
  (is (= 2 (:n-processed (:stats results))))
  (is (= 2 (:n-errored (:stats results))))
  (test-persistence "col-2" "id-b" {:events 0 :features 0})
  (test-persistence "col-2$nested" {:events 0 :features 0})
  (test-timeline "col-2" "id-b" {:timeline-current {:n 0}
                                 :timeline {:n 0}
                                 :timeline-changelog {}})
  (test-timeline->extract (:extracts results) 0)
  (test-geoserver "col-2" 0)
  (test-geoserver "col-2$nested" 0))

(defpermutatedtest double-nested-new-invalid-change-feature "col-2_id-b_double_nested_new_invalid_change" results
  (is (= (+ 3 1 2 2) (:n-processed (:stats results))))
  (is (= 5 (:n-errored (:stats results))))
  (test-persistence "col-2" "id-b" {:events 1 :features 1})
  (test-persistence "col-2$nestedserie" {:events 1 :features 1})
  (test-persistence "col-2$nestedserie$label" {:events 1 :features 1})
  (test-timeline "col-2" "id-b" {:timeline-current {:n 1}
                                 :timeline {:n 0}
                                 :timeline-changelog {:n-new 1 :n-change 2}})
  (test-timeline->extract (:extracts results) 1)
  (test-geoserver "col-2" 1)
  (test-geoserver "col-2$nestedserie" 0)
  (test-geoserver "col-2$nestedserie$label" 1))
