(ns pdok.random
  (:import com.fasterxml.uuid.Generators))

(def ^:private UUIDGenerator (Generators/timeBasedGenerator))

(defn UUID [] (.generate UUIDGenerator))
