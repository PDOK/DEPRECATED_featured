(ns pdok.featured.generator
  (:require [cheshire.core :as json]
            [clj-time [core :as t] [format :as tf] [local :refer [local-now]]])
  (:import  [java.io PipedInputStream PipedOutputStream]))

(def ^:dynamic *difficult-geometry?* false)

;; 2015-02-26T15:48:26.578Z
(def ^{:private true} date-time-formatter (tf/formatters :date-time) )

(def ^{:private true} date-formatter (tf/formatters :date) )

(defn random-word [length]
  (let [chars (map char (range 97 123))
        word (take length (repeatedly #(rand-nth chars)))]
    (reduce str word)))

(defn random-geometry []
  {:type "gml" :gml "<gml:Surface xmlns:gml=\"http://www.opengis.net/gml\" srsName=\"urn:ogc:def:crs:EPSG::28992\"><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\" count=\"5\">172307.599 509279.740 172307.349 509280.920 172306.379 509280.670 171306.699 508279.490 172307.599 509279.740</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>"})

(def difficult-geometry
  {:type "gml"
   :gml
   (str "<gml:Polygon xmlns:gml=\"http://www.opengis.net/gml\" srsName=\"urn:ogc:def:crs:EPSG::28992\" srsDimension=\"2\"> "
        "    <gml:exterior> "
        "        <gml:Ring> "
        "            <gml:curveMember> "
        "                <gml:Curve> "
        "                    <gml:segments> "
        "                        <gml:Arc> "
        "                            <gml:posList>159524.655 373757.311 159524.564 373756.801 159524.066 373756.866</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159524.066 373756.866 159522.084 373759.112 159520.195 373760.675</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159520.195 373760.675 159520.099 373761.185 159520.606 373761.283</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159520.606 373761.283 159510.395 373764.260 159500.684 373760.696</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159500.684 373760.696 159501.191 373760.609 159501.124 373760.115</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159501.124 373760.115 159498.342 373757.267 159496.409 373754.018</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159496.409 373754.018 159495.925 373753.843 159495.730 373754.307</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159495.730 373754.307 159494.431 373745.746 159497.448 373737.784</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159497.448 373737.784 159497.530 373738.286 159498.039 373738.217</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159498.039 373738.217 159499.803 373736.176 159501.744 373734.509</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159501.744 373734.509 159501.811 373733.995 159501.336 373733.919</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159501.336 373733.919 159511.727 373730.819 159522.601 373735.427</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159522.601 373735.427 159522.082 373735.450 159522.090 373735.956</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159522.090 373735.956 159523.740 373737.842 159525.053 373739.799</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159525.053 373739.799 159525.566 373739.933 159525.708 373739.453</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159525.708 373739.453 159527.748 373748.490 159524.655 373757.311</gml:posList> "
        "                        </gml:Arc> "
        "                    </gml:segments> "
        "                </gml:Curve> "
        "            </gml:curveMember> "
        "        </gml:Ring> "
        "    </gml:exterior> "
        "    <gml:interior> "
        "        <gml:Ring> "
        "            <gml:curveMember> "
        "                <gml:Curve> "
        "                    <gml:segments> "
        "                        <gml:Arc> "
        "                            <gml:posList>159518.638 373740.227 159503.916 373739.801 159503.160 373754.546</gml:posList> "
        "                        </gml:Arc> "
        "                        <gml:Arc> "
        "                            <gml:posList>159503.160 373754.546 159518.463 373755.013 159518.638 373740.227</gml:posList> "
        "                        </gml:Arc> "
        "                    </gml:segments> "
        "                </gml:Curve> "
        "            </gml:curveMember> "
        "        </gml:Ring> "
        "    </gml:interior> "
        "</gml:Polygon> ")})

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
    :_geometry (if *difficult-geometry?* difficult-geometry (random-geometry))})

(defn transform-to-change [feature]
  (-> feature
      (assoc :_action "change")
      (assoc :_current_validity (:_validity feature))
      (assoc :_validity (tf/unparse date-time-formatter
                          (t/plus (tf/parse date-time-formatter (:_validity feature)) (t/minutes 1))))))

(defn transform-to-close [feature]
  (-> feature
      (assoc :_action "close")
      (assoc :_current_validity (:_validity feature))
      (assoc :_validity (tf/unparse date-time-formatter
                          (t/plus (tf/parse date-time-formatter (:_validity feature)) (t/minutes 1))))))

(defn transform-to-delete [{:keys [_dataset _collection _id _validity]}]
  {:_action "delete"
   :_collection _collection
   :_id _id
   :_current_validity _validity})

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
  (update-an-attribute feature #(if %2 (assoc %1 %2 nil) %1)
                       [:_action :_collection :_id :_validity :_current_validity :_geometry]))

(defn random-new-feature
  ([collection attribute] (random-new-feature collection attribute (random-word 10)))
  ([collection attributes id]
   (let [feature (new-feature collection id)
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

(defn followed-by [feature change? close? delete? start-with-delete?]
  (let [change (fn [f] (selective-feature (transform-to-change f)))
        close  (fn [f] (selective-feature (transform-to-close f)))
        delete (fn [f] (selective-feature (transform-to-delete f)))
        result (cond-> []
                 start-with-delete? (conj (delete feature))
                 true (conj feature)
                 change? (conj (change feature))
                 close? (#(conj %1 (close (last %1))))
                 delete? (#(conj %1 (delete (last %1)))))]
    result))

(defn create-attributes [simple-attributes & {:keys [names]}]
  (let [attribute-names (or names (repeatedly simple-attributes #(random-word 5)))
        generators (cycle attribute-generators)
        attributes (map #(vector %1 %2) attribute-names generators)]
    attributes))

(defn random-json-features [out-stream dataset collection total & args]
  (let [{:keys [change? close? delete? start-with-delete? nested geometry? ids]
         :or {change? false close? false delete? false
              start-with-delete? false nested 0 geometry? true
              ids (repeatedly #(random-word 10))}} args
        validity (tf/unparse date-time-formatter (local-now))
        attributes (create-attributes 3)
        new-features (map #(random-new-feature collection attributes %) ids)
        with-nested (map #(combine-to-nested-feature % "nested" geometry?) (partition (+ 1 nested) new-features))
        with-extra (mapcat #(followed-by % change? close? delete? start-with-delete?) with-nested)
        package {:_meta {}
                 :dataset dataset
                 :features (take total with-extra)}]
    (json/generate-stream package out-stream)))

(defn- random-json-feature-stream* [out-stream dataset collection total & args]
  (with-open [writer (clojure.java.io/writer out-stream)]
    (apply random-json-features writer dataset collection total args)))

(defn random-json-feature-stream
  (^java.io.InputStream [dataset collection total & args]
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

(defn generate-test-files-with-deletes []
  (doseq [c [2 10 100 1000 10000 100000]]
    (with-open [w (clojure.java.io/writer (str ".test-files/delete-features-single-collection-" c ".json"))]
      (random-json-features w "deleteset" "collection1" c :delete? true))))

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
