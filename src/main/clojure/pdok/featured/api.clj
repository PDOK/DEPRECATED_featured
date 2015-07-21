(ns pdok.featured.api
  (:require [cheshire.core :as json]
            [clj-time [core :as t] [local :as tl]]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a
             :refer [>! <! >!! <!! go chan buffer close! thread
                     alts! alts!! timeout]]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [org.httpkit.client :as http]
            [pdok.featured
             [config :as config]
             [processor :as processor :refer [consume shutdown]]
             [json-reader :as reader]]
            [ring.middleware.defaults :refer :all]
            [ring.middleware.json :refer :all]
            [ring.util.response :as r]
            [schema.core :as s]))

(extend-protocol cheshire.generate/JSONable
  org.joda.time.DateTime
  (to-json [t jg] (.writeString jg (str t)))
  schema.utils.ValidationError
  (to-json [t jg] (.writeString jg (pr-str t))))

(defn uri [str]
  (try
    (let [uri (java.net.URI. str)]
      uri)
    (catch java.net.URISyntaxException e nil)))

(def URI (s/pred uri 'URI ))

(def ProcessRequest
  "A schema for a JSON process request"
  {:dataset s/Str
   :file URI
   (s/optional-key :callback) URI})

(defn- callbacker [uri run-stats]
  (http/post uri {:body (json/generate-string run-stats) :headers {"Content-Type" "application/json"}}))

(defn- process* [stats callback-chan request]
  (log/trace "Processsing: " request)
  (swap! stats assoc-in [:processing] request)
  (let [persistence (config/persistence)
        projectors (config/projectors persistence)
        processor (processor/create persistence projectors)]
    (try (with-open [in (io/input-stream (:file request))]
           (let [features (reader/features-from-stream in :dataset (:dataset request))
                 _ (dorun (consume processor features))
                 processor (shutdown processor)
                 run-stats (assoc (:statistics processor) :request request)]
             (swap! stats update-in [:processed] #(conj % run-stats))
             (swap! stats assoc-in [:processing] nil)
             (when (:callback request)
               (go (>! callback-chan [(:callback request) run-stats])))))
         (catch Exception e
           (let [error-stats (assoc request :error (str e))]
             (log/warn error-stats)
             (swap! stats update-in [:errored] #(conj % error-stats))
             (swap! stats assoc-in [:processing] nil)
             (when (:callback request)
               (go (>! callback-chan [(:callback request) error-stats])))))))
  )

(defn- process [process-chan http-req]
  (let [request (:body http-req)
        invalid (s/check ProcessRequest request)]
    (if invalid
      (r/status (r/response invalid) 400)
      (do (go (>! process-chan request)) (r/response {:result :ok})))))

(defn api-routes [process-chan callback-chan stats]
  (defroutes api-routes
    (context "/api" []
             (GET "/ping" [] (r/response {:pong (tl/local-now)}))
             (POST "/ping" [] (fn [r] (println "!ping pong!" (:body r)) (r/response {:pong (tl/local-now)})))
             (GET "/stats" [] (r/response @stats))
             (POST "/process" [] (partial process process-chan) ))
    (route/not-found "NOT FOUND")))

(defn rest-handler [& more]
  (let [pc (chan)
        cc (chan 10)
        stats (atom {:processing nil
                     :processed []
                     :errored []})]
    (go (while true (process* stats cc (<! pc))))
    (go (while true (apply callbacker (<! cc))))
    (-> (api-routes pc cc stats)
        (wrap-json-body {:keywords? true :bigdecimals? true})
        (wrap-json-response)
        (wrap-defaults api-defaults))))

(def app (routes (rest-handler)))
