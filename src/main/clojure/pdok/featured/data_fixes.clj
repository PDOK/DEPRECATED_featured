(ns pdok.featured.data-fixes
  (:require [pdok.featured.persistence :as pers]
            [pdok.featured.processor :as proc]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a]))

(defn close-childs [{:keys [persistence] :as processor} perform?]
  (let [collections (map :name
                         (filter #(not (:parent-collection %))
                                 (pers/collections persistence)))]
    (doseq [collection collections]
      (when (seq (pers/child-collections persistence collection))
        (log/info "Fixing:" collection)
        (let [n-closed (atom 0)
              stream-ids (pers/streams persistence collection)
              parts (a/pipe stream-ids (a/chan 1 (partition-all 10000)))]
          (loop [part (a/<!! parts)
                 i 1]
            (when (seq part)
              (pers/prepare persistence (map (fn [id] {:collection collection :id id}) part))
              (let [could-be-closed (filter #(= :close (pers/last-action persistence collection %)) part)
                    close-actions (map (fn [id] {:action            :close-childs
                                                 :parent-collection collection
                                                 :parent-id         id
                                                 :validity          (pers/current-validity persistence collection id)})
                                       could-be-closed)]
                (swap! n-closed #(+ % (count could-be-closed)))
                (when perform?
                  (dorun (proc/consume* processor close-actions))))
              (pers/flush persistence)
              (log/info "Total closed:" (:n-processed @(:statistics processor)))
              (log/info "Processed:" (* i 10000) "- Total closed parents:" @n-closed)
              (when (or perform? (> 5 i)) (recur (a/<!! parts) (inc i)))))
          (log/info "Completed:" collection "- Total closed" @n-closed "parents"))))))