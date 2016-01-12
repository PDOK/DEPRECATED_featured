(def feature-version (or (System/getenv "FEATURE_VERSION") "0.1"))
(def build-number (or (System/getenv "BUILD_NUMBER") "HANDBUILT"))
(def change-number (or (System/getenv "CHANGE_NUMBER") "031415"))
(def release-version (str feature-version "." build-number))
(def project-name "featured")
(def uberjar-name (str project-name "-standalone.jar"))
(def uberwar-name (str project-name ".war"))
(def git-ref (clojure.string/replace (:out (clojure.java.shell/sh "git" "rev-parse" "HEAD"))#"\n" "" ))

(create-ns 'pdok.lein)
(defn key->placeholder [k]
  (re-pattern (str "\\$\\{" (name k) "\\}")))

(defn generate-from-template [template-file replacement-map]
  (let [template (slurp template-file)
        replacements (map (fn [[k v]] [(key->placeholder k) (str v)]) replacement-map)]
    (reduce (fn [acc [k v]] (clojure.string/replace acc k v)) template replacements)))

(intern 'pdok.lein 'key->placeholder key->placeholder)
(intern 'pdok.lein 'generate-from-template generate-from-template)

(defproject featured release-version
  :uberjar-name ~uberjar-name
  :manifest {"Implementation-Version" ~(str release-version "(" git-ref ")")}
  :description "PDOK - No FME"
  :url "http://github.so.kadaster.nl/PDOK/featured"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["releases" "http://mvnrepository.so.kadaster.nl:8081/nexus/content/repositories/releases/"]
                 ["osgeo" {:url "http://download.osgeo.org/webdav/geotools/" :snapshots false}]
                 ["local" "file:repo"]]
    :mirrors {"*" {:name "kadaster"
                   :url "http://mvnrepository.so.kadaster.nl:8081/nexus/content/repositories/public/"}}
  :dependencies [[cheshire "5.4.0"]
                 [clj-time "0.9.0"]
                 [com.cognitect/transit-clj "0.8.269"]
                 [com.fasterxml.jackson.core/jackson-core "2.6.0"]
                 [com.fasterxml.uuid/java-uuid-generator "3.1.3"
                  :exclusions [[log4j]]]
                 [compojure "1.3.4"]
                 [stencil "0.4.0"]
                 [environ "1.0.1"]
                 [http-kit "2.1.18"]
                 [joplin.core "0.3.5"]
                 [joplin.jdbc "0.3.5"]
                 [org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.trace "0.7.8"]
                 [org.codehaus.woodstox/woodstox-core-asl "4.4.1"]
                 [org.geotools/gt-epsg-extension "13.0"]
                 [org.geotools/gt-shapefile "13.0"]
                 [org.geotools/gt-xml "13.0"]
                 [com.vividsolutions/jts "1.13"
                  :exclusions [[jts]]]
                 [log4j/log4j "1.2.17"]
                 [org.slf4j/slf4j-api "1.7.12"]
                 [org.slf4j/slf4j-log4j12 "1.7.12"]
                 [postgresql "9.3-1102.jdbc41"]
                 [prismatic/schema "0.4.3"]
                 [ring/ring-defaults "0.1.5"]
                 [ring/ring-json "0.3.1"]
                 [xalan/xalan "2.7.2"]
                 [nl.pdok/gml311-jts "0.0.3"]
                 [nl.pdok/rdnaptrans-java "1.0"]]
  :plugins [[lein-ring "0.9.6" ]
            [lein-filegen "0.1.0-SNAPSHOT"]]
  :ring {:handler pdok.featured.api/app
         :uberwar-name ~uberwar-name}
  :main ^:skip-aot pdok.featured.core
  :source-paths ["src/main/clojure"]
  :java-source-paths ["src/main/java"]
  :resource-paths ["config" "src/main/resources"]
  :test-paths ["src/test/clojure"]
  :test-selectors {:default (complement :regression)
                   :regression :regression}
  :filegen [{:data {:RELEASE_VERSION ~release-version :CHANGE_NUMBER ~change-number}
             :template-fn (partial pdok.lein/generate-from-template "deployit-manifest.xml.template")
             :target "target/deployit-manifest.xml"}
            {:data ~release-version
             :template-fn #(str "FEATURED_VERSION=" %1 "\n")
             :target "target/featured.properties"}
            {:data ~(str release-version "(" git-ref ")")
             :template-fn #(str %1)
             :target "src/main/resources/version"}]
  :aliases {"build" ["do" ["compile"] ["test"] ["filegen"]
                     ["ring" "uberwar"]]}
  :profiles {:uberjar {:aot :all}
             :test {:resource-paths ["src/test/resources"]
                    :dependencies [[org.clojure/math.combinatorics "0.1.1"]]
                    :jvm-opts ["-Dlog4j.configuration=no-logging.properties"]}
             :dev {:dependencies [[javax.servlet/servlet-api "2.5"]
                                  [ring-mock "0.1.5"]
                                  [org.clojure/math.combinatorics "0.1.1"]]
                   :resource-paths ["src/test/resources"]}})
