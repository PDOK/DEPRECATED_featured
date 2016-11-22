(ns pdok.util
  (:import [pdok.featured GeometryAttribute]
           [com.fasterxml.uuid UUIDComparator]))

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
