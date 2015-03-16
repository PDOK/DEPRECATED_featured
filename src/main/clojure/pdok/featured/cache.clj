(ns pdok.featured.cache
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
     (alter batch #(conj % args)))
    (if (<= batch-size (count @batch))
      (flush-batch batch batched-fn))))

(defn with-cache [cache cached-fn key-fn value-fn]
  (fn [& args]
    (dosync
     (alter cache #(cache/miss % (key-fn args) (value-fn args))))
    (apply cached-fn args)))

(defn apply-cache-miss-fn-result [cache key-value-pairs]
  (dosync
   (doseq [kvp key-value-pairs]
     (alter cache #(cache/miss % (first kvp) (second kvp))))))

(defn use-cache [cache cached-fn key-fn cache-miss-fn]
  (fn [& args]
    (letfn [(cache-lookup [key] (cache/lookup @cache key))]
      (let [cache-key (key-fn args)
            cached (cache-lookup cache-key)]
        (if cached
          cached
          (do (when cache-miss-fn (apply-cache-miss-fn-result cache (apply cache-miss-fn args)))
              (cache-lookup cache-key)))))))

(defmacro cached [{:keys [batch-size]} & body]
  `(let [cache# (ref (cache/basic-cache-factory {}))
         batch-size# (or ~batch-size 100000)
         batch# (ref (clojure.lang.PersistentQueue/EMPTY))
         ~'with-cache (partial with-cache cache#)
         ~'use-cache (partial use-cache cache#)
         ~'with-batch (partial with-batch batch# batch-size#)
         ~'flush-batch (partial flush-batch batch#)]
     ~@body))
