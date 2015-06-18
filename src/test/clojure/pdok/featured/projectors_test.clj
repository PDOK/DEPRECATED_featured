(ns pdok.featured.projectors-test
  (:require [clojure.test :refer :all]
            [pdok.featured.tiles :as tiles]
            [pdok.featured.projectors :refer :all]))


(def test-gml
    "<gml:Surface srsName=\"urn:ogc:def:crs:EPSG::28992\"><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension=\"2\" count=\"5\">172307.599 509279.740 172307.349 509280.920 172306.379 509280.670 171306.699 508279.490 172307.599 509279.740</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>"
 )

(def test-geometry {"gml" test-gml, "type" "gml"})

(def test-feature-with-geometry
       (merge
         {"_action" "new"}
         {:geometry test-geometry}
         {:other "foo"})
)

(def test-feature-without-geometry
       (merge
         {"_action" "new"}
         {:other "foo"})
)

(deftest test-nl-tiles-with-geometry
  (is (= #{49821 49864} (tiles/nl test-feature-with-geometry))))

(deftest test-nl-tiles-without-geometry
  (is (= nil (tiles/nl test-feature-without-geometry))))
