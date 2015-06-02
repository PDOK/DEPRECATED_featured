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

(defn- splitted-keys-with-default [splitted-keys]
  (let [feature-keys (:template-keys splitted-keys)
        feature-keys-with-default (assoc feature-keys (- (count feature-keys) 1) (str (last feature-keys) "_leeg"))]
    (assoc splitted-keys :template-keys feature-keys-with-default)
    ))

(defn- resolve-feature-value [feature-value template-function]
  (if (nil? template-function)
        feature-value
        (let [resolved-function (resolve-as-function template-function)
              resolved-value (resolved-function feature-value)]
         resolved-value)))

(defn- value-or-default-or-no-value [resolved-feature-value feature splitted-keys-with-default]
    (if (nil? resolved-feature-value)
      (or (get-in feature (:template-keys splitted-keys-with-default)) 
          "NO VALUE")
      resolved-feature-value))

(deftype MapProxy [feature]
   clojure.lang.IFn
      (invoke [_ k]
        (let [feature-key (clojure.string/replace k #"\{\{|\}\}" "")
              splitted-keys (split-feature-key feature-key)
              splitted-keys-with-default (splitted-keys-with-default splitted-keys)
              template-function (:template-function splitted-keys)
              feature-value (get-in feature (:template-keys splitted-keys))
              resolved-feature-value (resolve-feature-value feature-value template-function)]
          (str (value-or-default-or-no-value resolved-feature-value feature splitted-keys-with-default)))))

(defmulti render-template (fn [template features] 
  (if (instance? clojure.lang.IFn features) :single-feature :feature-collection)))

(defmethod render-template :single-feature [template feature]
  (let [pattern (template-to-pattern template)]
    (clojure.string/replace template pattern feature)))

(defmethod render-template :feature-collection [template features]
  (let [template (read-template template)]
    (map #(render-template template (->MapProxy %)) features)))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))