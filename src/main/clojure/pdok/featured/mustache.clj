(ns pdok.featured.mustache
   (:require [clostache.parser :as clostache]
             [pdok.featured.mustache-functions])
   (:gen-class))

(defn resolve-as-function [namespace function]
  (ns-resolve *ns* (symbol (str namespace "/" (name function)))))

(defn lookup-proxy [obj]
  (reify
    clojure.lang.ILookup
      (valAt [_ k]
           (if (and (map? obj)
                    (or (contains? obj k)(contains? obj (name k)))) 
             (let [value (get obj k (get obj (name k)))] 
                  (lookup-proxy value)) 
             (when-let [f (resolve-as-function "pdok.featured.mustache-functions" k)]
               (lookup-proxy (f obj)))))
      
    clojure.lang.IPersistentCollection 
      (cons [_ o](lookup-proxy (conj obj o)))
      (seq [this] (if (or (map? obj) (string? obj)) (list obj) (seq obj)))
    Object
      (toString [_] (str obj))))

(defn render [template feature]
  (clostache/render template (lookup-proxy feature)))

(defn render-resource [path feature] 
  (let [template (slurp path)]
    (render template feature)))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))