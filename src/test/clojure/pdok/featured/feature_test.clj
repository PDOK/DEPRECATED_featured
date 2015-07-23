(ns pdok.featured.feature-test
  (:require [clojure.test :refer :all]
            [pdok.featured.feature :refer :all]
            [clojure.java.io :refer :all])
  (:import [pdok.featured.xslt TransformXSLT]))


(def gml-with-hole "<gml:Polygon xmlns:gml=\"http://www.opengis.net/gml\" srsName=\"urn:ogc:def:crs:EPSG:28992\">
                   <gml:exterior><gml:Ring><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment>
                     <gml:posList srsDimension=\"2\"> 45000.0 65000.0 40000.0 65000.0 40000.0 60000.0 45000.0 60000.0 45000.0 65000.0 </gml:posList>
                   </gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:Ring></gml:exterior>
                   <gml:interior><gml:Ring><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment>
                     <gml:posList srsDimension=\"2\"> 43000.0 63000.0 42000.0 63000.0 42000.0 62000.0 43000.0 62000.0 43000.0 63000.0 </gml:posList>
                  </gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:Ring></gml:interior>
                   <gml:interior><gml:Ring><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment>
                     <gml:posList srsDimension=\"2\"> 44000.0 63000.0 42000.0 63000.0 42000.0 62000.0 43000.0 62000.0 44000.0 63000.0 </gml:posList>
                  </gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:Ring></gml:interior></gml:Polygon>")


(def gml-with-arc "<gml:Polygon xmlns:gml=\"http://www.opengis.net/gml\" srsName=\"urn:ogc:def:crs:EPSG:28992\">
                     <gml:exterior><gml:Ring><gml:curveMember><gml:Curve>
                                 <gml:segments>
                                    <gml:Arc><gml:posList srsDimension=\"2\"> 350.0 100.0 330.0 80.0 320.0 50.0 </gml:posList></gml:Arc>
                                    <gml:LineStringSegment><gml:posList srsDimension=\"2\"> 320.0 50.0 380.0 50.0 380.0 100.0 350.0 100.0 </gml:posList></gml:LineStringSegment>
                               </gml:segments></gml:Curve></gml:curveMember></gml:Ring></gml:exterior></gml:Polygon>")

(def gml-test "<gml:Polygon xmlns:gml=\"http://www.opengis.net/gml\" srsName=\"urn:ogc:def:crs:EPSG:28992\">
                     <gml:exterior><gml:Ring><gml:curveMember><gml:Curve>
                                 <gml:segments>
                                    <gml:LineStringSegment><gml:posList srsDimension=\"2\"> 350.0 100.0 330.0 80.0 320.0 50.0 320.0 50.0 380.0 50.0 380.0 100.0 350.0 100.0</gml:posList></gml:LineStringSegment>
                               </gml:segments></gml:Curve></gml:curveMember></gml:Ring></gml:exterior></gml:Polygon>")





(def gml-object-with-arc {"type" "gml" "gml" gml-with-arc})


(def gml-object-with-hole {"type" "gml" "gml" gml-with-hole})

(def gml-without-curves "<gml:Polygon xmlns:xlink=\"http://www.w3.org/1999/xlink\" xmlns:imgeo-s=\"http://www.geostandaarden.nl/imgeo/2.1/simple/gml31\" xmlns:gml=\"http://www.opengis.net/gml\"><gml:exterior><gml:LinearRing><gml:posList> 45000.0 65000.0 40000.0 65000.0 40000.0 60000.0 45000.0 60000.0 45000.0 65000.0 </gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList> 43000.0 63000.0 42000.0 63000.0 42000.0 62000.0 43000.0 62000.0 43000.0 63000.0 </gml:posList></gml:LinearRing></gml:interior><gml:interior><gml:LinearRing><gml:posList> 44000.0 63000.0 42000.0 63000.0 42000.0 62000.0 43000.0 62000.0 44000.0 63000.0 </gml:posList></gml:LinearRing></gml:interior></gml:Polygon>")
(def gml-object-without-curves {"type" "gml" "gml" gml-without-curves})

(defn test-transform [xsl] (transform xsl  gml-with-arc))

(deftest test-as-jts-with-curves
 (is (re-find #"curve" (as-gml gml-object-with-hole)))
 (is (not (re-find #"curve" (as-gml gml-object-without-curves))))
 (is (= (as-jts gml-object-with-hole) (as-jts gml-object-without-curves))))

(def gml-spoor "<gml:Curve srsName=\"urn:ogc:def:crs:EPSG::28992\">\n <gml:segments>\n <gml:LineStringSegment>\n <gml:posList srsDimension=\"2\" count=\"15\">076492.094 453702.905 076597.300 453750.312 076599.693 453751.441 076602.252 453752.711 076605.353 453754.372 076608.285 453756.106 076611.245 453758.008 076614.154 453759.983 076628.035 453769.741 076630.984 453771.756 076633.898 453773.622 076636.980 453775.425 076640.113 453777.138 076669.505 453792.674 076765.562 453843.434</gml:posList>\n </gml:LineStringSegment>\n </gml:segments>\n </gml:Curve>\n")


(deftest test-gml-spoor
  (let [transformed-gml (-> gml-spoor strip-gml-ns transform-curves)]
  (is (re-find #"<LineStringSegment" transformed-gml))
  (is (re-find #"</LineStringSegment>" transformed-gml))
  (is (re-find #"076492.094 453702.905 076597.300 453750.312 076599.693 453751.441 076602.252 453752.711 076605.353 453754.372 076608.285 453756.106 076611.245 453758.008 076614.154 453759.983 076628.035 453769.741 076630.984 453771.756 076633.898 453773.622 076636.980 453775.425 076640.113 453777.138 076669.505 453792.674 076765.562 453843.434" transformed-gml))))

(def gml-surface "<gml:Surface srsName=\"urn:ogc:def:crs:EPSG::28992\">
            <gml:patches>
              <gml:PolygonPatch>
                <gml:exterior>
                  <gml:Ring>
                    <gml:curveMember>
                      <gml:LineString>
                        <gml:posList srsDimension=\"2\" count=\"5\">196305.082 391991.976 196284.313 391985.153 196265.021 391979.033 196263.249 391978.549 196225.502 391965.932</gml:posList>
                      </gml:LineString>
                    </gml:curveMember>
                    <gml:curveMember>
                      <gml:Curve>
                        <gml:segments>
                          <gml:Arc>
                            <gml:posList srsDimension=\"2\" count=\"3\">196225.502 391965.932 196225.061 391965.140 196224.480 391964.440</gml:posList>
                           </gml:Arc>
                         </gml:segments>
                       </gml:Curve>
                      </gml:curveMember>
                      <gml:curveMember>
                        <gml:LineString>
                          <gml:posList srsDimension=\"2\" count=\"21\">196224.480 391964.440 196245.583 391969.969 196245.593 391969.888 196265.997 391975.495 196272.044 391977.157 196272.294 391976.245 196273.299 391976.509 196285.910 391980.635 196320.390 391991.917 196349.913 392001.452 196357.445 392003.885 196365.313 392006.983 196398.317 392019.981 196397.546 392023.530 196397.964 392023.711 196398.502 392025.057 196397.543 392024.664 196358.867 392008.781 196350.687 392006.075 196305.312 391991.277 196305.082 391991.976</gml:posList>
                       </gml:LineString>
                      </gml:curveMember>
                    </gml:Ring>
                  </gml:exterior>
                </gml:PolygonPatch>
              </gml:patches>
            </gml:Surface>")

(deftest test-gml-surface 
  (let [transformed-gml (-> gml-surface strip-gml-ns transform-curves)]
    (is (re-find #"<Polygon" transformed-gml))
    (is (re-find #"196305.082 391991.976" transformed-gml))
    (is (re-find #"196225.502 391965.932" transformed-gml))
    (is (re-find #"196224.480 391964.440" transformed-gml))))
  
  ; (def transformed-gml (transform (TransformXSLT. (clojure.java.io/input-stream (clojure.java.io/resource "pdok/featured/xslt/curve2linearring.xsl"))) (strip-gml-ns gml-surface)))
  ; (parse-to-jts transformed-gml)
  
