(ns pdok.featured.json-reader-test
  (:require [pdok.featured.json-reader :refer :all]
            [clojure.test :refer :all]
            [clojure.java.io :as io]))

(deftest test-parse-geo-attr
  (let [geo-string "<gml:Point xmlns:gml=\"http://www.opengis.net/gml/3.2\" srsName=\"urn:ogc:def:crs:EPSG::28992\" gml:id=\"LOCAL_ID_1\"><gml:pos>131411.071 481481.517</gml:pos></gml:Point>"
        geo-format "gml"
        result (parse-geo-attr geo-format geo-string)]
    (is (= {"type" geo-format geo-format geo-string} result))
    (is (= {:geo-attr true} (meta result)))))
