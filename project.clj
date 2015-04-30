(defproject featured "0.1.0-SNAPSHOT"
  :description "PDOK - No FME"
  :url "http://github.so.kadaster.nl/PDOK/featured"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["osgeo" {:url "http://download.osgeo.org/webdav/geotools/"
                           :snapshots false}]]
  :dependencies [[org.clojure/clojure "1.7.0-alpha5"]
                 [org.clojure/tools.trace "0.7.8"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [environ "1.0.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [com.fasterxml.jackson.core/jackson-core "2.4.4"]
                 [com.fasterxml.uuid/java-uuid-generator "3.1.3"]
                 [clj-time "0.9.0"]
                 [cheshire "5.4.0"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [postgresql "9.3-1102.jdbc41"]
                 [com.cognitect/transit-clj "0.8.269"]
                 [org.geotools/gt-xml "13.0"]
                 [org.geotools/gt-epsg-extension "13.0"]
                 [org.geotools/gt-shapefile "13.0"]
                 [org.codehaus.woodstox/woodstox-core-asl "4.4.1"]]
  :plugins [[lein-environ "1.0.0"]]
  :main ^:skip-aot pdok.featured.core
  :target-path "target/%s"
  :source-paths ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :resource-paths ["resources" "src/main/resources"]
  :test-paths ["src/test/clojure"]
  :profiles {:uberjar {:aot :all}})
