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
             [extracts :as extracts]
             [template :as template]
             [json-reader :as reader]
             [zipfiles :as zipfiles]]
            [ring.middleware.defaults :refer :all]
            [ring.middleware.json :refer :all]
            [ring.util.response :as r]
            [schema.core :as s])
   )

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
   (s/optional-key :format) (s/enum "json" "zip")
   (s/optional-key :processingOptions) [{:collection s/Str
                                         :options [(s/enum "no-visualization")]}]
   (s/optional-key :callback) URI})

(def ExtractRequest
  "A schema for a JSON extract request"
  {:dataset s/Str
   :collection s/Str
   :extractType s/Str
   (s/optional-key :callback) URI})

(def TemplateRequest
  "A schema for a JSON template request"
  {:dataset s/Str
   :extractType s/Str
   :templateName s/Str
   :template s/Str})

(def FlushRequest
  "A schema for a JSON flush request"
  {:dataset s/Str})

(defn- callbacker [uri run-stats]
  (http/post uri {:body (json/generate-string run-stats) :headers {"Content-Type" "application/json"}}))


(defn- stats-on-callback [callback-chan request stats]
  (when (:callback request)
          (go (>! callback-chan [(:callback request) stats]))))

(defn collections-with-option [filter-option processing-options]
  (map :collection (filter (fn [p] (some #(= filter-option %) (:options p))) processing-options)))

(defn- process* [stats callback-chan request]
  (log/info "Processsing: " request)
  (swap! stats assoc-in [:processing] request)
  (let [persistence (config/persistence)
        projectors [(config/projectors persistence
                                       :no-visualization (collections-with-option "no-visualization" (:processingOptions request)))
                    (config/timeline persistence)]
        processor (processor/create persistence projectors)
        zip-file? (= (:format request) "zip")]
    (try
          (with-open [input (io/input-stream (:file request))]
            (let [_ (log/info "processing file: " (:file request))
                  in (if zip-file? (zipfiles/zip-as-input input) input)
                  [meta features] (reader/features-from-stream in :dataset (:dataset request))
                  processor (merge processor meta) ;; ugly, should move init here, but that doesnt work for the catch
                  _ (dorun (consume processor features))
                  _ (if zip-file? (zipfiles/close-zip in))
                 processor (shutdown processor)
                 run-stats (assoc (:statistics processor) :request request)]
             (swap! stats update-in [:processed] #(conj % run-stats))
             (swap! stats assoc-in [:processing] nil)
             (stats-on-callback callback-chan request run-stats)))
         (catch Exception e
           (let [ _ (log/error e)
                 processor (shutdown processor)
                 error-stats (assoc request :error (str e))]
             (log/warn error-stats)
             (swap! stats update-in [:errored] #(conj % error-stats))
             (swap! stats assoc-in [:processing] nil)
             (stats-on-callback callback-chan request error-stats)))))
  )


(defn- process-request [schema request-chan http-req]
  (let [request (:body http-req)
        invalid (s/check schema request)]
    (if invalid
      (r/status (r/response invalid) 400)
      (do (go (>! request-chan request)) (r/response {:result :ok})))))

(defn- extract* [callback-chan request]
  (log/info "Processing extract: " request)

  (try
    (let [response (extracts/fill-extract (:dataset request)
                                          (:collection request)
                                          (:extractType request))
          _ (log/info "response: " response)
          extract-stats (assoc request :response response)]
       (stats-on-callback callback-chan request extract-stats))
    (catch Exception e
      (let [error-stats (assoc request :response {:status "error" :msg (str e)})]
        (log/warn error-stats)
        (stats-on-callback callback-chan request error-stats)))))


(defn- template-request [http-req]
  (let [request (:body http-req)
        invalid (s/check TemplateRequest request)]
    (if invalid
      (r/status (r/response invalid) 400)
      (r/response (if (template/add-or-update-template {:dataset (:dataset request)
                                                    :extract-type (:extractType request)
                                                    :name (:templateName request)
                                                    :template (:template request)})
                    {:status "ok"}
                    {:status "error"})))))

(defn- flush-extract-delta [http-req]
  (let [request (:body http-req)
        invalid (s/check FlushRequest request)]
    (if invalid
      (r/status (r/response invalid) 400)
      (r/response (extracts/flush-delta (:dataset request))))))


(defn api-routes [process-chan extract-chan callback-chan stats]
  (defroutes api-routes
    (context "/api" []
             (GET "/info" [] (r/response {:version (-> (eval 'pdok.featured.core) .getPackage .getImplementationVersion)}))
             (GET "/ping" [] (r/response {:pong (tl/local-now)}))
             (POST "/ping" [] (fn [r] (log/info "!ping pong!" (:body r)) (r/response {:pong (tl/local-now)})))
             (GET "/stats" [] (r/response @stats))
             (POST "/process" [] (partial process-request ProcessRequest process-chan))
             (POST "/extract" [] (partial process-request ExtractRequest extract-chan))
             (POST "/extract/flush-delta" [] (fn [r] (r/response (flush-extract-delta r))))
             (POST "/template" [] (fn [r] (r/response (template-request r)))))
    (route/not-found "NOT FOUND")))

(defn rest-handler [& more]
  (let [pc (chan)
        ec (chan)
        cc (chan 10)
        stats (atom {:processing nil
                     :processed []
                     :errored []})]
    (go (while true (process* stats cc (<! pc))))
    (go (while true (extract* cc (<! ec))))
    (go (while true (apply callbacker (<! cc))))
    (-> (api-routes pc ec cc stats)
        (wrap-json-body {:keywords? true :bigdecimals? true})
        (wrap-json-response)
        (wrap-defaults api-defaults))))

(def app (routes (rest-handler)))
