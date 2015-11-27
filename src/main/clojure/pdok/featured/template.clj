(ns pdok.featured.template
  (:require [pdok.featured.mustache :as m]
            [clojure.java.io :as io]))


(defn- template-qualifier [dataset extract-type]
  (str dataset "-" extract-type "-" ))

(defn template-key [dataset extract-type name]
  (str (template-qualifier dataset extract-type) name))

(defn- template-with-metadata* [dataset file]
  (let [file-name (.getName file)]
    {:dataset dataset
     :extract-type (cond-> file 
                           (-> file .getParentFile .getName (= "partials")) .getParentFile
                           true .getParentFile 
                           true .getName) 
     :name (subs file-name 0 (.lastIndexOf file-name ".")) 
     :template (slurp (.getPath file))}))

(defn templates-with-metadata [dataset template-location]
    "Returns the template-contents with metadata included. 
     The metadata is based on dataset and where template-file in direcotry structure is located."
  (let [template-files (filter #(.isFile %) (file-seq (io/file template-location)))]
    (map (partial template-with-metadata* dataset) template-files)))

(defn add-or-update-template [{:keys [dataset extract-type name template]}]
  (let [template (m/replace-in-template template (template-qualifier dataset extract-type) "{{>")]
    (m/register (template-key dataset extract-type name) template)))





