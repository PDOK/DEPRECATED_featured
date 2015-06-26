(ns pdok.featured.api
  (:require [pdok.featured
             [config :as config]
             [processor :as processor :refer [consume shutdown]]
             [json-reader :as reader]]
            [compojure.core :refer :all]
            [compojure.handler :as handler]
            [compojure.route :as route]
            [ring.util.response :as r]
            [cheshire.core :as json]
            [clj-time.local :as tl]
            [clojure.core.async :as a
             :refer [>! <! >!! <!! go chan buffer close! thread
                     alts! alts!! timeout]]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [environ.core :refer [env]]
            [clojure.java.jdbc :as j]
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
   :file URI})

(defn- process* [stats request]
  (swap! stats assoc-in [:processing] request)
  (let [persistence (config/persistence)
        projectors (config/projectors persistence)
        processor (processor/create persistence projectors)]
    (with-open [in (io/input-stream (:file request))]
      (let [features (reader/features-from-stream in :dataset (:dataset request))
            consumed (consume processor features)
            n-processed (count consumed)]
        (shutdown processor)
        (swap! stats update-in [:processed] #(conj % (assoc request :n-processed n-processed)))
        (swap! stats assoc-in [:processing] nil))))
  )

(defn- process [process-chan http-req]
  (let [request (:body http-req)
        invalid (s/check ProcessRequest request)]
    (if invalid
      (r/status (r/response invalid) 400)
      (do (go (>! process-chan request)) (r/response {:result :ok})))))

(defn api-routes [process-chan stats]
  (defroutes api-routes
    (context "/api" []
             (GET "/ping" [] (r/response {:pong (tl/local-now)}))
             (GET "/stats" [] (r/response @stats))
             (POST "/process" [] (partial process process-chan) ))
    (route/not-found "NOT FOUND")))

(defn rest-handler [args]
  (let [c (chan)
        stats (atom {:processing nil
                     :processed []})]
    (go (while true (process* stats (<! c))))
    (handler/api (api-routes c stats)))
      )
