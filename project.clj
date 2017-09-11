(def version (slurp "VERSION"))
(def artifact-name (str "featured-" version))
(def uberjar-name (str artifact-name "-standalone.jar"))
(def webjar-name (str artifact-name "-web.jar"))
(def uberwar-name (str artifact-name ".war"))
(def git-ref (clojure.string/replace (:out (clojure.java.shell/sh "git" "rev-parse" "HEAD"))#"\n" "" ))

(defproject featured version
  :min-lein-version "2.5.4"
  :uberjar-name ~uberjar-name
  :manifest {"Implementation-Version" ~(str version "(" git-ref ")")}
  :description "PDOK - No FME"
  :url "http://github.com/PDOK/featured"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["osgeo" {:url "http://download.osgeo.org/webdav/geotools/" :snapshots false}]
                 ["local" "file:repo"]
                 ["oss mirror" "https://oss.sonatype.org/content/groups/public/"]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [cheshire "5.5.0"]
                 [clj-time "0.11.0"]
                 [com.cognitect/transit-clj "0.8.288"]
                 [com.fasterxml.jackson.core/jackson-core "2.6.0"]
                 [com.fasterxml.uuid/java-uuid-generator "3.1.3"
                  :exclusions [[log4j]]]
                 [compojure "1.4.0" :exclusions [[org.clojure/tools.reader]]]
                 [stencil "0.4.0"]
                 [environ "1.0.1"]
                 [http-kit "2.1.18"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [org.clojure/tools.reader "1.0.0-alpha2"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.trace "0.7.8"]
                 [org.geotools/gt-shapefile "13.0"]
                 [log4j/log4j "1.2.17"]
                 [org.slf4j/slf4j-api "1.7.12"]
                 [org.slf4j/slf4j-log4j12 "1.7.12"]
                 [org.postgresql/postgresql "9.4.1209.jre7"]
                 [prismatic/schema "0.4.3"]
                 [ring/ring-defaults "0.1.5"]
                 [ring/ring-json "0.3.1"]
                 [nl.pdok/featured-shared "1.1.4"]]
  :plugins [[lein-ring "0.9.6" ]
            [pdok/lein-filegen "0.1.0"]]
  :ring {:port 8000
         :handler pdok.featured.api/app
         :init pdok.featured.api/init!
         :uberwar-name ~uberwar-name}
  :main ^:skip-aot pdok.featured.core
  :source-paths ["src/main/clojure"]
  :resource-paths ["config" "src/main/resources"]
  :test-paths ["src/test/clojure"]
  :test-selectors {:default (fn [m] (not (or (:performance m) (:regression m))))
                   :regression :regression
                   :performance :performance}
  :filegen [{:data ~(str version "(" git-ref ")")
             :template-fn #(str %1)
             :target "src/main/resources/version"}]
  :profiles {:uberjar {:aot :all}
             :cli {:uberjar-name ~uberjar-name
                   :aliases {"build" ["do" "uberjar"]}}
             :web-war {:aliases {"build" ["do" "filegen" ["ring" "uberwar"]]}}
             :web-jar {:uberjar-name ~webjar-name
                       :aliases {"build" ["do" "filegen" ["ring" "uberjar"]]}}
             :test {:resource-paths ["src/test/resources"]
                    :dependencies [[org.clojure/math.combinatorics "0.1.1"]]
                    :jvm-opts ["-Xmx3072M" "-Dlog4j.configuration=no-logging.properties"]}
             :dev {:dependencies [[javax.servlet/servlet-api "2.5"]
                                  [ring-mock "0.1.5"]
                                  [org.clojure/math.combinatorics "0.1.1"]]
                   :resource-paths ["src/test/resources"]}})
