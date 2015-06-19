(ns pdok.featured.generator
  (:require [cheshire.core :as json]
            [clj-time [core :as t] [format :as tf] [local :refer [local-now]]])
  (:import  [java.io PipedInputStream PipedOutputStream]))

;; 2015-02-26T15:48:26.578Z
(def ^{:private true} date-time-formatter (tf/formatters :date-time) )

(def ^{:private true} date-formatter (tf/formatters :date) )

(defn random-word [length]
  (let [chars (map char (range 97 123))
        word (take length (repeatedly #(rand-nth chars)))]
    (reduce str word)))

(defn random-geometry []
  {:type "gml" :gml "<gml:Surface srsName=\"urn:ogc:def:crs:EPSG::28992\"><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\" count=\"5\">172307.599 509279.740 172307.349 509280.920 172306.379 509280.670 171306.699 508279.490 172307.599 509279.740</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>"})

(defn random-date []
  (let [date (local-now)
        date-string (tf/unparse date-formatter date)]
    ["~#date", [date-string]]))

(defn random-moment []
  (let [moment (local-now)
        date-time-string (tf/unparse date-time-formatter moment)]
    ["~#moment", [date-time-string]]))

(def attribute-generators [#(random-word 5) random-date random-moment])

(defn new-feature [collection id]
   {:_action "new"
      :_collection collection
      :_id id
      :_validity (tf/unparse date-time-formatter (local-now))
      :_geometry (random-geometry)})

(defn transform-to-change [feature]
  (-> feature
      (assoc :_action "change")
      (assoc :_current_validity (:_validity feature))
      (assoc :_validity (tf/unparse date-time-formatter (t/plus (local-now) (t/minutes 1))))))

(defn transform-to-close [feature]
  (-> feature
      (assoc :_action "close")
      (assoc :_current_validity (:_validity feature))
      (assoc :_validity (tf/unparse date-time-formatter (t/plus (local-now) (t/minutes 1))))))

(defn transform-to-nested [feature]
  (apply dissoc feature [:_action :_collection :_id :_validity :_current_validity]))

(defn add-attribute [feature key generator]
  (assoc feature key (generator)))


(defn update-an-attribute [feature update-fn exceptions]
  (let [valid-keys (keys (apply dissoc feature exceptions))
        key (rand-nth valid-keys)]
    (update-fn feature key)))

(defn remove-an-attribute [feature]
  (update-an-attribute feature #(dissoc %1 %2)
                       [:_action :_collection :_id :_validity :_current_validity]))

(defn nillify-an-attribute [feature]
  (update-an-attribute feature #(assoc %1 %2 nil)
                       [:_action :_collection :_id :_validity :_current_validity :_geometry]))

(defn random-new-feature
  ([collection attributes]
   (let [id (random-word 10)
         feature (new-feature collection id)
         feature (reduce (fn [acc [attr generator]] (add-attribute acc attr generator)) feature attributes)]
      feature)))

(defn selective-feature
  ([feature]
   (let [result (-> feature
                     remove-an-attribute
                     nillify-an-attribute)]
     result)))

(defn combine-to-nested-feature [[base & rest-features] attr top-geometry?]
  (let [base (if top-geometry? base (dissoc base :_geometry))
        nested-features (map transform-to-nested rest-features)]
    (case (count nested-features)
      0 base
      1 (assoc base attr (first nested-features))
      (assoc base attr (into [] nested-features)))))

(defn followed-by [feature change? close?]
  (let [change (fn [f] (selective-feature (transform-to-change f)))
        close  (fn [f] (selective-feature (transform-to-close f)))
        result [feature]
        result (if change?
                 (conj result (change feature))
                 result)
        result (if close?
                 (conj result (close feature))
                 result)]
    result))

(defn create-attributes [simple-attributes & {:keys [names]}]
  (let [attribute-names (or names (repeatedly simple-attributes #(random-word 5)))
        generators (cycle attribute-generators)
        attributes (map #(vector %1 %2) attribute-names generators)]
    attributes))

(defn random-json-features [out-stream dataset collection total & args]
  (let [{:keys [change? close? nested geometry?] :or {change? false close? false nested 0 geometry? true}} args
        validity (tf/unparse date-time-formatter (local-now))
        attributes (create-attributes 3)
        new-features (repeatedly #(random-new-feature collection attributes))
        with-nested (map #(combine-to-nested-feature % "nested" geometry?) (partition (+ 1 nested) new-features))
        with-extra (mapcat #(followed-by % change? close?) with-nested)
        package {:_meta {}
                 :dataset dataset
                 :features (take total with-extra)}]
    (json/generate-stream package out-stream)))

(defn- random-json-feature-stream* [out-stream dataset collection total & args]
  (with-open [writer (clojure.java.io/writer out-stream)]
    (apply random-json-features writer dataset collection total args)))

(defn random-json-feature-stream
  ([dataset collection total & args]
   (let [pipe-in (PipedInputStream.)
         pipe-out (PipedOutputStream. pipe-in)]
     (future (apply random-json-feature-stream* pipe-out dataset collection total args))
     pipe-in)))

(defn generate-test-files []
  (doseq [c [10 100 1000 10000 100000]]
    (with-open [w (clojure.java.io/writer (str ".test-files/new-features-single-collection-" c ".json"))]
      (random-json-features w "newset" "collection1" c))))

(defn generate-test-files-with-changes []
  (doseq [c [10 100 1000 10000 100000]]
    (with-open [w (clojure.java.io/writer (str ".test-files/change-features-single-collection-" c ".json"))]
      (random-json-features w "changeset" "collection1" c :change? true))))

(defn generate-test-files-with-closes []
  (doseq [c [10 100 1000 10000 100000]]
    (with-open [w (clojure.java.io/writer (str ".test-files/close-features-single-collection-" c ".json"))]
      (random-json-features w "closeset" "collection1" c :close? true))))

(defn generate-test-files-with-nested-feature []
  (doseq [c [5 50 500 5000 50000]]
    (with-open [w (clojure.java.io/writer (str ".test-files/new-features-nested-feature-" c ".json"))]
      (random-json-features w "nestedset" "collection1" c :nested 1))))

(defn generate-test-files-with-nested-features []
  (doseq [c [3 33 333 3333 33333]]
    (with-open [w (clojure.java.io/writer (str ".test-files/new-features-nested-features-" c ".json"))]
      (random-json-features w "nestedset" "collection1" c :nested 2))))

(defn generate-test-files-with-nested-feature-no-top-geometry []
  (doseq [c [10 100 1000 10000 100000]]
    (with-open [w (clojure.java.io/writer (str ".test-files/new-features-nested-feature-no-top-geometry-" c ".json"))]
      (random-json-features w "nestedset" "collection1" c :nested 1 :geometry? false))))

(defn generate-test-files-with-nested-features-no-top-geometry []
  (doseq [c [5 50 500 5000 50000]]
    (with-open [w (clojure.java.io/writer (str ".test-files/new-features-nested-features-no-top-geometry-" c ".json"))]
      (random-json-features w "nestedset" "collection1" c :nested 2 :geometry? false))))
