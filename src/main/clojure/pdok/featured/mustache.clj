(ns pdok.featured.mustache
  (:require [clostache.parser :as clostache]
            [pdok.featured.mustache-functions])
  (:gen-class))

(defn resolve-as-function [namespace function]
  (ns-resolve *ns* (symbol (str namespace "/" (name function)))))

(declare collection-proxy lookup-proxy)

(defn mustache-proxy [value]
  (if (sequential? value)
    (collection-proxy value)
    (lookup-proxy value)))


(defn val-at[k obj]
  (if (and (map? obj) (or (contains? obj k) (contains? obj (name k))))
    (let [value (get obj k (get obj (name k)))]
       (mustache-proxy value))
    (when-let [f (resolve-as-function "pdok.featured.mustache-functions" k)]
       (mustache-proxy (f obj)))))

(defn lookup-proxy [obj]
  (reify
    clojure.lang.ILookup
      (valAt [_ k] (val-at k obj))
    clojure.lang.IPersistentCollection
      (cons [_ o](lookup-proxy (conj obj o)))
      (seq [this] (if-not (clojure.string/blank? (str obj)) (list obj) nil))
    Object
      (toString [_] (str obj))))


(defn collection-proxy [obj]
  (reify
    clojure.lang.ILookup
      (valAt [_ k] (val-at k obj))
    clojure.lang.IPersistentCollection
      (cons [_ o](mustache-proxy (conj obj o)))
      (seq [this] (if (or (map? obj) (string? obj)) (list obj) (seq obj)))
    Object
      (toString [_] (str obj))))

(defn render [template feature partials]
   (if (nil? partials)
     (clostache/render template (lookup-proxy feature))
     (clostache/render template (lookup-proxy feature) partials)))

(defn render-resource
  ([path feature]
     (render-resource path feature nil))
  ([path feature partials]
     (let [template (slurp path)]
       (render template feature partials))))


;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
