(ns pdok.cache
  (:require [clojure.core.cache :as cache]))

(defn flush-batch [batch batched-fn]
  (def records (map identity @batch))
  (dosync
   (ref-set batch clojure.lang.PersistentQueue/EMPTY))
  (when (not-empty records)
    (batched-fn records)))

(defn with-batch [batch batch-size batched-fn]
  (fn [& args]
    (dosync
     (if (= 1 (count args))
       (alter batch #(conj % (first args)))
       (alter batch #(conj % args))))
    (if (<= batch-size (count @batch))
      (flush-batch batch batched-fn))))

(defn with-cache [cache cached-fn key-fn value-fn]
  (fn [& args]
    (dosync
     (when-let [key (apply key-fn args)]
       (when (some (complement nil?) key)
         (let [current-value (cache/lookup @cache key)]
           (alter cache #(cache/miss % key (apply value-fn current-value args)))))))
    (apply cached-fn args)))

(defn apply-cache-miss-fn-result [cache key-value-pairs]
  (dosync
   (doseq [kvp key-value-pairs]
     (alter cache #(cache/miss % (first kvp) (second kvp))))))

(defn use-cache [cache key-fn cache-miss-fn]
  (fn [& args]
    (letfn [(cache-lookup [key] (cache/lookup @cache key))]
      (let [cache-key (apply key-fn args)
            cached (cache-lookup cache-key)]
        (if cached
          cached
          (do (when cache-miss-fn (apply-cache-miss-fn-result cache (apply cache-miss-fn args)))
              (cache-lookup cache-key)))))))

(defmacro cached [cache f & args]
  "Cached version of f. Needs atom as cache. If first param is :reload reloads"
  `(let [fn-name# (name '~f)]
     (fn ([& args#]
         (let [f# (partial ~f ~@args)
               reload?# (= :reload (first args#))
               fn-args# (if reload?# (rest args#) args#)
               cache-key# (apply conj [fn-name#] fn-args#)]
            (if-let [e# (and (not reload?#) (find @~cache cache-key#))]
              (val e#)
              (let [ret# (apply f# fn-args#)]
                (swap! ~cache assoc cache-key# ret#)
                ret#)))))))
