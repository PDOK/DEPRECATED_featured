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

(deftype MapProxy [feature]
   clojure.lang.IFn
      (invoke [_ k]
             (let [feature-key (clojure.string/replace k #"\{\{|\}\}" "")]
              (if (= "geometry#gml" feature-key)
                  (let [render-function (str feature-key)
                        render-function (last (clojure.string/split render-function #"\#"))]
                    (or ((resolve-as-function render-function) (:geometry feature)) "not defined"))

                  (or (str (.valAt feature feature-key)) "not defined")))))


(defmulti render-template (fn [template features] 
  (if (instance? clojure.lang.IFn features) :single-feature :feature-collection)))

(defmethod render-template :single-feature [template feature]
  (let [pattern (template-to-pattern template)]
  (clojure.string/replace template pattern feature)))

(defmethod render-template :feature-collection [template features]
  (let [template (read-template template)]
    (map #(render-template template (->MapProxy %)) features)))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))