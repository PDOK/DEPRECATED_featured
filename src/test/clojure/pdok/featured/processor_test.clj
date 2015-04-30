(ns pdok.featured.processor-test
  (:require [clj-time.local :as tl]
            [pdok.featured.processor :refer :all]
            [pdok.featured.persistence :as pers]
            [pdok.featured.projectors :as proj]
            [clojure.test :refer :all]))

(defrecord MockedPersistence [streams-n appends-n exists? current-validity last-action]
  pers/ProcessorPersistence
  (init [this] (assoc this :initialized true))
  (stream-exists? [this dataset collection id] exists?)
  (create-stream [this dataset collection id]
    (pers/create-stream this dataset collection id nil nil))
  (create-stream [this dataset collection id parent-collection parent-id]
    (swap! streams-n inc))
  (append-to-stream [this version action dataset collection id validity geometry attributes]
    (swap! appends-n inc))
  (current-validity [_ dataset collection id]
    current-validity)
  (last-action [this dataset collection id]
    last-action)
  (close [this] (assoc this :closed true)))

(defrecord MockedProjector [features-n changes-n]
  proj/Projector
  (init [this] (assoc this :initialized true))
  (new-feature [_ feature]
    (swap! features-n inc))
  (change-feature [_ feature]
    (swap! changes-n inc))
  (close [this] (assoc this :closed true)))

(defn create-persistence [] (->MockedPersistence (atom 0) (atom 0) false nil nil))

(defn create-projectors [n]
  (repeatedly n #(->MockedProjector (atom 0) (atom 0))))

(defn create-processor
  ([] (create-processor 1))
  ([n-projectors]
    {:persistence (create-persistence)
     :projectors (create-projectors n-projectors)}))

(def default-validity (tl/local-now))

(def valid-new-feature
  {:action :new
   :dataset "known-dataset"
   :collection "collection-1"
   :id "valid-feature"
   :validity default-validity
   :geometry {:type "dummy"}
   :attributes {:field1 "test"}})

(def valid-change-feature
  {:action :change
   :dataset "known-dataset"
   :collection "collection1"
   :id "valid-feature"
   :validity default-validity
   :current-validity default-validity
   :geometry {:type "dummy"}
   :attributes {:field2 "test"}})

(deftest inititialized-processor
  (let [processor (processor (create-persistence) (create-projectors 2))]
    (testing "Initialized persistence?"
      (is (-> processor :persistence :initialized)))
    (testing "Initialized projectors?"
      (doseq [p (:projectors processor)]
        (is (-> p :initialized))))))

(deftest shutdown-processor
  (let [processor (shutdown (create-processor 2))]
    (testing "Closed persistence?"
      (is (-> processor :persistence :closed)))
    (testing "Closed projectors?"
      (doseq [p (:projectors processor)]
        (is (-> p :closed))))))

(deftest ok-new-feature
  (let [processor (create-processor)
        processed (first (consume processor valid-new-feature))]
    (is (not (nil? processed)))
    (is (not (:invalid? processed)))
    (is (= 1 @(-> processor :persistence :streams-n)))
    (is (= 1 @(-> processor :persistence :appends-n)))
    (is (= 1 @(-> processor :projectors first :features-n)))))

(def make-broken-new-transformations
  [["no dataset" #(dissoc % :dataset)]
   ["empty dataset" #(assoc % :dataset "")]
   ["no collection" #(dissoc % :collection)]
   ["empty collection" #(assoc % :collection "")]
   ["no id" #(dissoc % :id)]
   ["empty id" #(assoc % :id "")]
   ["no validity" #(dissoc % :id)]])

(deftest nok-new-feature
  (doseq [[name transform] make-broken-new-transformations]
    (let [processor (create-processor)
          invalid-new-feature (transform valid-new-feature)
          processed (first(consume processor invalid-new-feature))]
      (testing (str name " should break")
        (is (not (nil? processed)))
        (is (:invalid? processed))
        (is (= 0 @(-> processor :persistence :streams-n)))
        (is (= 0 @(-> processor :persistence :appends-n)))
        (is (= 0 @(-> processor :projectors first :features-n)))))))

(deftest ok-change-feature
  (let [processor (-> (create-processor)
                      (assoc-in [:persistence :current-validity] default-validity)
                      (assoc-in [:persistence :exists?] true))
        processed (first (consume processor valid-change-feature))]
    (is (not (nil? processed)))
    (is (not (:invalid? processed)) (str (:invalid-reasons processed)))
    (is (= 0 @(-> processor :persistence :streams-n))) ; no new stream
    (is (= 1 @(-> processor :persistence :appends-n)))
    (is (= 0 @(-> processor :projectors first :features-n))) ; no new features
    (is (= 1 @(-> processor :projectors first :changes-n)))
    ))

(def make-broken-change-transformations
  [["no current-validity" #(dissoc % :current-validity)]
   ["wrong current-validity" #(assoc % :current-validity (tl/local-now))]])

(deftest nok-change-feature
  (doseq [[name transform] (concat make-broken-new-transformations
                                   make-broken-change-transformations)]
    (let [processor (-> (create-processor)
                        (assoc-in [:persistence :current-validity] default-validity)
                        (assoc-in [:persistence :exists?] true))
          invalid-change-feature (transform valid-change-feature)
          processed (first (consume processor invalid-change-feature))]
      (testing (str name " should break")
        (is (not (nil? processed)))
        (is (:invalid? processed))
        (is (= 0 @(-> processor :persistence :appends-n)))
        (is (= 0 @(-> processor :projectors first :features-n))) ; no new features
        (is (= 0 @(-> processor :projectors first :changes-n))))
      )))
