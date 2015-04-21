(ns pdok.featured.mustache
   (:require [clojure.java.io :as io]
             [pdok.featured.feature  :refer :all]))

(defn read-template [template]
  (slurp (io/resource template)))

(defn variables-from-template [template]
  distinct (re-seq #"\{\{[\S]*\}\}" template))

(defn template-to-pattern [template]
  (let [ variable-list  (distinct (re-seq #"\{\{[\S]*\}\}" template))
         pattern (str (apply str (interpose "|" (map #(java.util.regex.Pattern/quote %) variable-list))))]
    (re-pattern pattern)))

(defn resolve-as-function [function]
  (ns-resolve *ns* (symbol (str "pdok.featured.feature/as-" function))))

(deftype MapProxy [feature]
   clojure.lang.IFn
      (invoke [_ k]
             (let [feature-key (keyword (clojure.string/replace k #"\{\{|\}\}" ""))]
              (if (= :_geometry.gml feature-key)
                  (let [render-function (str feature-key)
                        render-function (last (clojure.string/split render-function #"\."))]
                   
                    ((resolve-as-function render-function) (:_geometry feature)))

                  (or (.valAt feature feature-key) "")))))

(defmulti render-template (fn [template features] (if (instance? clojure.lang.IFn features) :single-feature :feature-collection)))

(defmethod render-template :single-feature [template feature] 
  (clojure.string/replace template (template-to-pattern template) feature))

(defmethod render-template :feature-collection [template features]
  (let [template (read-template template)]
    (map #(render-template template (->MapProxy %)) features)))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))