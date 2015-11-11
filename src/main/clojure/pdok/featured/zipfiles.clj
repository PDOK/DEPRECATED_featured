(ns pdok.featured.zipfiles
  (:require  [clojure.tools.logging :as log])
  (:import [java.util.zip ZipInputStream]))

(defn zip-as-input [inputstream] 
  (let [zip (ZipInputStream. inputstream)
        ze (.getNextEntry zip)]
  (if (nil? ze) 
    (throw (Exception. "No entries in zipfile."))
    zip)))


(defn close-zip [zip]
    (do (log/info "close zip")
      	(.closeEntry zip)
        (.close zip)))           