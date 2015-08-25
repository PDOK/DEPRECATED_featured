(ns pdok.featured.zipfiles
  (:import [java.util.zip ZipInputStream]))

(defn zip-as-input [input] 
  (let [zip (ZipInputStream. input)
        ze (.getNextEntry zip)]
  (if (nil? ze) 
    (throw (Exception. "No entries in zipfile."))
    zip)))

(defn close-zip [zip]
    (do (.closeEntry zip)
        (.close zip)))           