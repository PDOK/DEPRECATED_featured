(ns pdok.cache
  (:require [clojure.core.cache :as cache]))

(defn process-batch [batch consumer]
  (let [records (map identity @batch)]
    (when (not-empty records)
      (consumer records))))

(defn flush-batch [batch consumer]
  (let [records (map identity @batch)]
    (vreset! batch clojure.lang.PersistentQueue/EMPTY)
    (when (not-empty records)
      (consumer records))))

(defn batched
  ([batch batch-size type f]
   (if (= :batch-fn type)
     (batched batch batch-size #(flush-batch batch f))
     (batched batch batch-size f)))
  ([batch batch-size flush-fn]
   (fn [& args]
     (if (= 1 (count args))
       (vswap! batch #(conj % (first args)))
       (vswap! batch #(conj % args)))
     (if (<= batch-size (count @batch))
       (flush-fn)))))

(defn with-cache [cache cached-fn key-fn value-fn]
  (fn [& args]
    (when-let [key (apply key-fn args)]
      (when (some (complement nil?) key)
        (let [current-value (cache/lookup @cache key)]
          (vswap! cache #(cache/miss % key (apply value-fn current-value args))))))
    (apply cached-fn args)))

(defn apply-to-cache [cache key-value-pairs]
  (doseq [kvp key-value-pairs]
    (vswap! cache #(cache/miss % (first kvp) (second kvp)))))

(defn use-cache
  ([cache key-fn] (use-cache cache key-fn nil))
  ([cache key-fn cache-miss-fn]
   (fn [& args]
     (letfn [(cache-lookup [key] (cache/lookup @cache key))]
       (let [cache-key (apply key-fn args)
             cached (cache-lookup cache-key)]
         (if cached
           cached
           (do (when cache-miss-fn (apply-to-cache cache (apply cache-miss-fn args)))
               (cache-lookup cache-key))))))))

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

(defn once-true-fn
  "Returns a function which returns true once for an argument"
  []
  (let [mem (atom {})]
    (fn [& args]
      (if-let [e (find @mem args)]
        false
        (do
          (swap! mem assoc args nil)
          true)))))
