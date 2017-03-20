(ns pdok.featured.timeline-test
  (:require [pdok.featured.timeline :as timeline]
            [clojure.test :refer :all])
  (:import (pdok.featured GeometryAttribute)))

(def ^:private c "house")
(def ^:private id "h001")
(def ^:private v1 "00000000-0000-0000-0000-000000000000")
(def ^:private v2 "00000000-0000-0000-0000-000000000001")
(def ^:private d1 "2017-01-01T12:00:00.000")
(def ^:private d2 "2017-01-02T12:00:00.000")
(def ^:private t1 [12345])
(def ^:private t2 [54321])
(def ^:private g1 (GeometryAttribute. "gml" "0.0 1.0" (int 28992)))
(def ^:private g2 (GeometryAttribute. "gml" "1.0 0.0" (int 28992)))
(def ^:private s1 "first")
(def ^:private s2 "second")

(deftest merge-on-new
  "Test if a feature with action 'new' leads to the expected timeline format."
  (let [feature {:action "new"
                 :collection c
                 :id id
                 :version v1
                 :validity d1
                 :tiles t1
                 :attributes {:_geometry g1
                              :attr-a s1}}
        after {:_collection c
               :_id id
               :_version v1
               :_all_versions [v1]
               :_valid_from d1
               :_tiles t1
               :_geometry g1
               :attr-a s1}
        merged (#'timeline/build-new-entry feature)]
    (is (= after merged))))

(deftest merge-on-change
  "Test if a feature with action 'change' leads to the expected timeline format.
  There are no merge semantics for unspecified fields, only before's _all_versions field is used."
  (let [before {:_collection c
                :_id id
                :_version v1
                :_all_versions [v1]
                :_valid_from d1
                :_tiles t1
                :_geometry g1
                :attr-a s1}
        feature {:action "change"
                 :collection c
                 :id id
                 :version v2
                 :validity d2
                 :tiles t2
                 :attributes {:_geometry g2
                              :attr-b s2}}
        after {:_collection c
               :_id id
               :_version v2
               :_all_versions [v1, v2]
               :_valid_from d2
               :_tiles t2
               :_geometry g2
               :attr-b s2}
        merged (#'timeline/build-change-entry before feature)]
    (is (= after merged))))

(deftest merge-on-close
  "Test if a feature with action 'close' leads to the expected timeline format.
  Tiles and attributes are taken from the existing version, the feature's validity is used as _valid_to."
  (let [before {:_collection c
                :_id id
                :_version v1
                :_all_versions [v1]
                :_valid_from d1
                :_tiles t1
                :_geometry g1
                :attr-a s1}
        feature {:action "close"
                 :collection c
                 :id id
                 :version v2
                 :validity d2
                 :tiles []
                 :attributes {}}
        after {:_collection c
               :_id id
               :_version v2
               :_all_versions [v1, v2]
               :_valid_from d1
               :_valid_to d2
               :_tiles t1
               :_geometry g1
               :attr-a s1}
        merged (#'timeline/build-close-entry before feature)]
    (is (= after merged))))
