(def feature-version (or (System/getenv "FEATURE_VERSION") "0.1"))
(def build-version (or (System/getenv "BUILD_NUMBER") "HANDBUILT"))
(def release-version (str feature-version "." build-version))
(def project-name "featured")

(defproject featured release-version
  :uberjar-name ~(str project-name "-" release-version "-standalone.jar")
  :manifest {"Implementation-Version" ~release-version}
  :description "PDOK - No FME"
  :url "http://github.so.kadaster.nl/PDOK/featured"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["osgeo" {:url "http://download.osgeo.org/webdav/geotools/"
                           :snapshots false}]]
  :mirrors {"*" {:name "kadaster"
                 :url "http://mvnrepository.so.kadaster.nl:8081/nexus/content/repositories/public/"}}
  :dependencies [[cheshire "5.4.0"]
                 [clj-time "0.9.0"]
                 [com.cognitect/transit-clj "0.8.269"]
                 [com.fasterxml.jackson.core/jackson-core "2.4.4"]
                 [com.fasterxml.uuid/java-uuid-generator "3.1.3"]
                 [compojure "1.3.4"]
                 [de.ubercode.clostache/clostache "1.4.0"]
                 [environ "1.0.0"]
                 [http-kit "2.1.18"]
                 [org.clojure/clojure "1.7.0-alpha5"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.trace "0.7.8"]
                 [org.codehaus.woodstox/woodstox-core-asl "4.4.1"]
                 [org.geotools/gt-epsg-extension "13.0"]
                 [org.geotools/gt-shapefile "13.0"]
                 [org.geotools/gt-xml "13.0"]
                 [postgresql "9.3-1102.jdbc41"]
                 [prismatic/schema "0.4.3"]
                 [ring/ring-defaults "0.1.2"]
                 [ring/ring-json "0.3.1"]
                 [xalan/xalan "2.7.2"]]
  :plugins [[lein-environ "1.0.0"]
            [lein-ring "0.9.6" ]]
  :ring {:handler pdok.featured.core/app
         :uberwar-name ~(str project-name "-" release-version "-standalone.war")}
  :main ^:skip-aot pdok.featured.core
  :target-path "target/%s"
  :source-paths ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :resource-paths ["resources" "src/main/resources"]
  :test-paths ["src/test/clojure"]
  :aliases {"build" ["do" ["compile"] ["test"]
                     ["ring" "uberwar"]]}
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies [[javax.servlet/servlet-api "2.5"]
                                  [ring-mock "0.1.5"]]}})
