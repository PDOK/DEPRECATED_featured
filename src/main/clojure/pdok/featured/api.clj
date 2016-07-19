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
             [zipfiles :as zipfiles]
             [persistence :as persistence]]
            [ring.middleware.defaults :refer :all]
            [ring.middleware.json :refer :all]
            [ring.util.response :as r]
            [schema.core :as s])
  (:import [com.fasterxml.jackson.core JsonGenerator]
           (clojure.lang PersistentQueue)
           (org.joda.time DateTime)
           (schema.utils ValidationError)
           (java.io File)))

(extend-protocol cheshire.generate/JSONable
  DateTime
  (to-json [t ^JsonGenerator jg] (.writeString jg (str t)))
  ValidationError
  (to-json [t ^JsonGenerator jg] (.writeString jg (pr-str t))))

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
   (s/optional-key :callback) URI
   (s/optional-key :no-timeline) boolean
   (s/optional-key :no-state) boolean
   (s/optional-key :projection) s/Str})

(def ExtractRequest
  "A schema for a JSON extract request"
  {:dataset s/Str
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

(defn download-file [uri zipped?]
  "returns [file err]"
  (try
    (let [tmp (File/createTempFile "featured" (if zipped? ".zip" ".json"))
          in (io/input-stream uri)]
      (log/info "Downloading" uri)
      (io/copy in tmp)
      (.close in)
      (if zipped?
        (do
          (log/info "Extracting" uri)
          (let [entry (zipfiles/first-file-from-zip tmp)]
            (io/delete-file tmp)
            [entry nil]))
        [tmp nil]))
    (catch Exception e
      [nil (:cause e)])))

(defn- process* [worker-id stats callback-chan request]
  (log/info "Processsing: " request)
  (swap! stats assoc-in [:processing worker-id] request)
  (swap! stats update-in [:queued] pop)
  (let [dataset (:dataset request)
        persistence (if (:no-state request) (persistence/make-no-state) (config/persistence))
        projectors (cond-> [(config/projectors persistence
                                               :projection (:projection request)
                                               :no-visualization (collections-with-option "no-visualization" (:processingOptions request)))]
                    (not (:no-timeline request)) (conj (config/timeline persistence)))
        processor (processor/create dataset persistence projectors)
        zipped? (= (:format request) "zip")
        [file err] (download-file (:file request) zipped?)]
    (if-not file
      (do
        (swap! stats assoc-in [:processing worker-id] nil)
        (stats-on-callback callback-chan request
                           (assoc request :error
                                  (if err err "Somethin went wrong downloading"))))
      (try
        (with-open [in (io/input-stream file)]
          (let [_ (log/info "processing file: " (:file request))
                [meta features] (reader/features-from-stream in :dataset (:dataset request))
                processor (merge processor meta) ;; ugly, should move init here, but that doesnt work for the catch
                _ (dorun (consume processor features))
                processor (shutdown processor)
                run-stats (assoc (:statistics processor) :request request)]
            (swap! stats assoc-in [:processing worker-id] nil)
            (stats-on-callback callback-chan request run-stats)))
        (catch Exception e
          (let [ _ (log/error e)
                processor (shutdown processor)
                error-stats (assoc request :error (str e))]
            (log/warn e error-stats)
            (swap! stats assoc-in [:processing worker-id] nil)
            (stats-on-callback callback-chan request error-stats)))
        (finally (io/delete-file file))))))

(defn- process-request [stats schema queue-id request-chan http-req]
  (let [request (:body http-req)
        invalid (s/check schema request)]
    (if invalid
      (r/status (r/response invalid) 400)
      (if (a/offer! request-chan request)
        (do (swap! stats update-in [queue-id] #(conj % request)) (r/response {:result :ok}))
        (r/status (r/response {:error "queue full"}) 429)))))

(defn- extract* [stats callback-chan request]
  (log/info "Processing extract: " request)
  (swap! stats update-in [:extract-queue] pop)
  (try
    (let [response (extracts/fill-extract (:dataset request)
                                          (:extractType request))
          _ (log/info "response: " response)
          extract-stats (merge request response)]
       (stats-on-callback callback-chan request extract-stats))
    (catch Exception e
      (let [error-stats (merge request {:status "error" :msg (str e)})]
        (log/warn e error-stats)
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

(defn- flush-extract-changelog [http-req]
  (let [request (:body http-req)
        invalid (s/check FlushRequest request)]
    (if invalid
      (r/status (r/response invalid) 400)
      (r/response (extracts/flush-changelog (:dataset request))))))


(defn api-routes [process-chan extract-chan stats]
  (defroutes api-routes
    (context "/api" []
             (GET "/info" [] (r/response {:version (slurp (clojure.java.io/resource "version"))}))
             (GET "/ping" [] (r/response {:pong (tl/local-now)}))
             (POST "/ping" [] (fn [r] (log/info "!ping pong!" (:body r)) (r/response {:pong (tl/local-now)})))
             (GET "/stats" [] (r/response @stats))
             (POST "/process" [] (partial process-request stats ProcessRequest :queued process-chan))
             (POST "/extract" [] (partial process-request stats ExtractRequest :extract-queue extract-chan))
             (POST "/extract/flush-changelog" [] (fn [r] (r/response (flush-extract-changelog r))))
             (POST "/template" [] (fn [r] (r/response (template-request r)))))
    (route/not-found "NOT FOUND")))

(defn wrap-exception-handling
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception e
        (log/error e)
        {:status 400 :body (.getMessage e)}))))

(defn create-workers [stats callback-chan process-chan]
  (let [factory-fn (fn [worker-id]
                     (swap! stats assoc-in [:processing worker-id] nil)
                     (log/info "Creating worker " worker-id)
                     (go (while true (process* worker-id stats callback-chan (<! process-chan)))))]
    (config/create-workers factory-fn)))

(defn rest-handler [& more]
  (let [pc (chan 1000)
        ec (chan 100)
        cc (chan 10)
        stats (atom {:processing {}
                     :queued     (PersistentQueue/EMPTY)
                     :extract-queue (PersistentQueue/EMPTY)})]
    (create-workers stats cc pc)
    (go (while true (extract* stats cc (<! ec))))
    (go (while true (apply callbacker (<! cc))))
    (-> (api-routes pc ec stats)
        (wrap-json-body {:keywords? true :bigdecimals? true})
        (wrap-json-response)
        (wrap-defaults api-defaults)
        (wrap-exception-handling))))

(def app (routes (rest-handler)))
