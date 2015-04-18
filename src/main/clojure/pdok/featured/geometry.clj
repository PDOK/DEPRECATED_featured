(ns pdok.featured.geometry
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

(defn replace-template [template f-key-value]
  (clojure.string/replace template (template-to-pattern template) f-key-value))

(deftype MapProxy [feature]
    clojure.lang.IFn
    	(invoke [_ k]
             (let [key-in-feature (keyword (clojure.string/replace k #"\{\{|\}\}" ""))]
      				(if (= :_geometry.gml key-in-feature)
            			;TODO split key and resolve function based on key
                    (as-gml (:_geometry feature))
                   ; ((ns-resolve *ns* (symbol "pdok.featured.feature/as-gml")) (:_geometry feature))

                 		(or (.valAt feature key-in-feature)
          				"")))))

(defn replace-features-in-template [template features]
  (let [ template (read-template template)
         _ (println template)]
         (for [feature (into [] features)]  
           (replace-template template (->MapProxy feature)))))

;(with-open [s (file-stream ".test-files/new-features-single-collection-100000.json")] (time (last (features-from-package-stream s))))