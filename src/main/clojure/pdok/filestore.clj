(ns pdok.filestore
  (:require [clojure.java.io :as io]
            [clj-time.core :as time]
            clj-time.coerce)
  (:import (java.io File)))

(defn safe-delete [file-path]
  (if (.exists (io/file file-path))
    (try
      (clojure.java.io/delete-file file-path)
      (catch Exception e (str "exception: " (.getMessage e))))
    false))

(defn delete-files [files]
  (doseq [file files]
    (safe-delete file)))

(defn cleanup-old-files [filestore threshold-seconds]
  (let [files (.listFiles ^File (:store-dir filestore))
        threshold (clj-time.coerce/to-long (time/minus (time/now) (time/seconds threshold-seconds)))
        old-files (filter #(< (.lastModified %) threshold) files)]
    (delete-files old-files)
    old-files))

(defn create-target-file [filestore name]
  (let [file (io/file (:store-dir filestore) name)]
    (when (.exists file) (throw (Exception. (str "File: " name " already exists"))))
    file))

(defn get-file [filestore name]
  (let [file (io/file (:store-dir filestore) name)]
    (when (.exists file) file)))
