(ns pdok.util
  (:require [clj-time [format :as tf] [coerce :as tc]])
  (:import [com.fasterxml.uuid UUIDComparator]
           [org.joda.time DateTimeZone]))

(def ^:private uuid-comparator (UUIDComparator.))

(defn uuid> [uuid-a uuid-b]
  (> (.compare ^UUIDComparator uuid-comparator uuid-a uuid-b) 0))

(defn uuid>= [uuid-a uuid-b]
  (>= (.compare ^UUIDComparator uuid-comparator uuid-a uuid-b) 0))

(defn uuid< [uuid-a uuid-b]
  (< (.compare ^UUIDComparator uuid-comparator uuid-a uuid-b) 0))

(defn uuid<= [uuid-a uuid-b]
  (<= 0 (.compare ^UUIDComparator uuid-comparator uuid-a uuid-b) 0))

(defmacro checked [f on-error-check]
  "Executes f and on errors will check if error-check is ok. If not throws original error."
  `(try ~f
        (catch Exception e#
         (when (not ~on-error-check) (throw e#)))))

(defn partitioned [f n coll]
  "executes f with max n elemenents from coll"
  (let [parts (partition-all n coll)
        results (map f parts)]
    (mapcat identity results)))

(defmacro with-bench
  "Evaluates expr, followed by bench-out (setting t to time it took in ms) and returning expr"
  [t bench-out & expr]
  `(let [start# (. System (nanoTime))
         ret# (do ~@expr)
         ~t (/ (double (- (. System (nanoTime)) start#)) 1000000.0)]
     ~bench-out
     ret#))

;; 2015-02-26T15:48:26.578Z
(def ^{:private true} date-time-formatter (tf/with-zone (tf/formatters :date-time-parser) (DateTimeZone/forID "Europe/Amsterdam")))

(def ^{:private true} date-formatter (tf/formatter "yyyy-MM-dd"))

(defn parse-time
  "Parses an ISO8601 date timestring to local date time"
  [datetimestring]
  (when-not (clojure.string/blank? datetimestring)
    (tc/to-local-date-time (tf/parse date-time-formatter datetimestring))))

(defn parse-date
  "Parses a date string to local date"
  [datestring]
  (when-not (clojure.string/blank? datestring)
    (tc/to-local-date (tf/parse date-formatter datestring))))
