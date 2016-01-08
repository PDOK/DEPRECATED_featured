(ns pdok.random
  (:import com.fasterxml.uuid.Generators))

(def ^:private UUIDGenerator (Generators/timeBasedGenerator))

(defn ordered-UUID [] (.generate UUIDGenerator))
