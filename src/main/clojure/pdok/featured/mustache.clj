(ns pdok.featured.mustache
  (:require [stencil.core :as stencil]
            [stencil.loader :as loader]
            [pdok.featured.mustache-functions]
            [clojure.tools.logging :as log])
  (:gen-class))

(defn resolve-as-function [namespace function]
  (ns-resolve *ns* (symbol (str namespace "/" (name function)))))

(declare collection-proxy lookup-proxy)

(defn mustache-proxy [value]
  (if (sequential? value)
    (collection-proxy value)
    (lookup-proxy value)))

(defn val-at[k obj]
  (if-let [f (resolve-as-function "pdok.featured.mustache-functions" k)]
    (mustache-proxy (f obj))
    (if (and (map? obj) (or (contains? obj k) (contains? obj (name k))))
      (let [value (get obj k (get obj (name k)))]
        (if (or (= (class value) pdok.featured.feature.NilAttribute) (= value nil))
          nil
          (mustache-proxy value))))))

(defn lookup-proxy [obj]
  (reify
    clojure.lang.ILookup
      (valAt [_ k] (val-at k nil))
      (valAt [_ k not-found] (if-let [v (val-at k obj)] v not-found))
    clojure.lang.IPersistentCollection
      (cons [_ o](lookup-proxy (conj obj o)))
      (seq [this] (if-not (clojure.string/blank? (str obj)) (list obj) nil))
    clojure.lang.Associative
      (containsKey [_ key] (if (val-at key obj) true false))
    Object
      (toString [_] (str obj))))


(defn collection-proxy [obj]
  (reify
    clojure.lang.Sequential
    clojure.lang.ILookup
      (valAt [_ k] (val-at k obj))
    clojure.lang.IPersistentCollection
      (cons [_ o](mustache-proxy (conj obj o)))
      (seq [this] (if (or (map? obj) (string? obj)) (list (mustache-proxy obj)) (seq (map mustache-proxy obj))))
    Object
      (toString [_] (str obj))))


(defn replace-in-template [template qualifier search-prefix]
 (clojure.string/replace template search-prefix (str search-prefix qualifier)))

(def ^{:private true} registered-templates (atom #{}))
(def ^{:private true} registered-partials (atom #{}))

(defn register [registered name template]
  (if-not (contains? @registered name)
    (do 
      (loader/register-template name template)
      (swap! registered conj name))))

(defn register-partial [name template]
   (if-not (contains? @registered-partials name)
    (do 
      (loader/register-template name template)
      (swap! registered-partials conj name))))

(defn render
  ([template feature] (render template nil))
  ([{:keys [name template]} feature partials]
   (if-not (nil? partials)
     (doseq [[k partial] partials] (let [name (:name partial)
                                         partial-content (:template partial)]
                                     (register registered-partials name partial-content))))
   (register registered-templates name template)
   (stencil/render-file name (lookup-proxy feature))))

(defn render-resource
  ([path feature]
   (stencil/render-file path (lookup-proxy feature))))


;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))
