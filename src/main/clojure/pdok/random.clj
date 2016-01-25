(ns pdok.random
  (:import [com.fasterxml.uuid Generators]
           [com.fasterxml.uuid.impl TimeBasedGenerator]))

(def ^:private UUIDGenerator (Generators/timeBasedGenerator))

(defn ordered-UUID [] (locking UUIDGenerator (.generate ^TimeBasedGenerator UUIDGenerator)))
