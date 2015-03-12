(defproject featured "0.1.0-SNAPSHOT"
  :description "PDOK - No FME"
  :url "http://github.so.kadaster.nl/PDOK/featured"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0-alpha5"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [environ "1.0.0"]
                 [com.fasterxml.jackson.core/jackson-core "2.4.4"]
                 [clj-time "0.9.0"]
                 [cheshire "5.4.0"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [postgresql "9.3-1102.jdbc41"]
                 [com.vividsolutions/jts "1.13" :exclusions [xerces/xercesImpl]]]
  :main ^:skip-aot featured.core
  :target-path "target/%s"
  :source-paths ["src/main/clojure"]
  :test-paths ["src/test/clojure"]
  :profiles {:uberjar {:aot :all}})
