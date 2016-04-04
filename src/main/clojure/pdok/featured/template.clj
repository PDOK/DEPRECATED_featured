(ns pdok.featured.template
  (:require [pdok.featured.mustache :as m]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.tools.logging :as log]))


(defn- template-qualifier [dataset-name extract-type]
  (str dataset-name "-" extract-type "-" ))

(defn template-key [dataset-name extract-type name]
  (str (template-qualifier dataset-name extract-type) name))

(defn- template-with-metadata* [dataset-name ^java.io.File file]
  (let [file-name (.getName file)]
    {:dataset-name dataset-name
     :extract-type (cond-> file
                     (-> file .getParentFile .getName (= "partials")) .getParentFile
                     true .getParentFile
                     true .getName)
     :name (subs file-name 0 (.lastIndexOf file-name "."))
     :template (slurp (.getPath file))}))

(defn templates-with-metadata [dataset-name template-location]
    "Returns the template-contents with metadata included.
     The metadata is based on dataset and where template-file in direcotry structure is located."
    (let [template-files (filter (fn [^java.io.File f] (.isFile f)) (file-seq (io/file template-location)))]
    (map (partial template-with-metadata* dataset-name) template-files)))

(defn- clean-up [source]
    (s/trim (reduce str (s/split-lines (s/trim-newline (s/replace source #"\t" ""))))))

(defn add-or-update-template [{:keys [dataset-name extract-type name template]}]
  (let [template (clean-up template)
        template (m/replace-in-template template (template-qualifier dataset-name extract-type) "{{>")
        template-key (template-key dataset-name extract-type name)]
      (try
        (m/register template-key template)
        (m/render template-key {}) ;check template can be rendered with empty map
        {:template template-key :status "registered"}
      (catch Exception e
        (log/error "Template: " template-key " cannot be registered")
        (log/error e)
        false))))
