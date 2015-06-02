(ns pdok.featured.mustache
   (:require [clojure.java.io :as io]
             [pdok.featured.feature  :refer :all]))

(defn read-template [template]
  (slurp (io/resource template)))

(defn variables-from-template [template]
  distinct (re-seq #"\{\{[\S]*\}\}" template))

(defn template-to-pattern [template]
  (let [ variable-list  (distinct (re-seq #"\{\{[^\s\{\}]*\}\}" template))
         pattern (str (apply str (interpose "|" (map #(java.util.regex.Pattern/quote %) variable-list))))]
    (re-pattern pattern)))

(defn resolve-as-function [function]
  (ns-resolve *ns* (symbol (str "pdok.featured.feature/as-" function))))

(defn- template-keys [feature-key](let [splitted (clojure.string/split feature-key #"\.")]
  (into [] (filter #(not (.startsWith % "#")) splitted ))))

(defn- template-function [feature-key]
  (let [template-function (first (filter #(.startsWith % "#") (clojure.string/split feature-key #"\.")))]
    (if (nil? template-function) nil (subs template-function 1))))

(defn split-feature-key [feature-key] 
  { :template-keys (template-keys feature-key)
    :template-function (template-function feature-key)})

(deftype MapProxy [feature]
   clojure.lang.IFn
      (invoke [_ k]
             (let [feature-key (clojure.string/replace k #"\{\{|\}\}" "")
                   splitted-keys (split-feature-key feature-key)
                   template-function (:template-function splitted-keys)
                   feature-value (get-in feature (:template-keys splitted-keys))]
                 (if (nil? template-function) 
                     (or (str feature-value) "not defined")
                     (let [resolved-function (resolve-as-function template-function)
                           resolved-value (resolved-function feature-value)]
                      (or (str resolved-value) "not defined (with function)"))))))

(defmulti render-template (fn [template features] 
  (if (instance? clojure.lang.IFn features) :single-feature :feature-collection)))

(defmethod render-template :single-feature [template feature]
  (let [pattern (template-to-pattern template)]
    (clojure.string/replace template pattern feature)))

(defmethod render-template :feature-collection [template features]
  (let [template (read-template template)]
    (map #(render-template template (->MapProxy %)) features)))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))