(ns pdok.featured.api
  (:require [cheshire.core :as json]
            [clj-time [local :as tl]]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a
             :refer [>! <! >!! <!! go chan buffer close! thread
                     alts! alts!! timeout]]
            [clojure.java.io :as io]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [org.httpkit.client :as http]
            [pdok.featured
             [config :as config]
             [processor :as processor :refer [consume shutdown]]
             [json-reader :as reader]
             [zipfiles :as zipfiles]
             [persistence :as persistence]]
            [ring.middleware.defaults :refer :all]
            [ring.middleware.json :refer :all]
            [ring.util.response :as r]
            [schema.core :as s]
            [pdok.filestore :as fs])
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
   (s/optional-key :delivery-info) s/Any
   (s/optional-key :processingOptions) [{:collection s/Str
                                         :options [(s/enum "no-visualization")]}]
   (s/optional-key :callback) URI
   (s/optional-key :no-timeline) boolean
   (s/optional-key :no-state) boolean
   (s/optional-key :projection) s/Str})

(defn- callbacker [uri run-stats]
  (try
    (http/post uri {:body (json/generate-string run-stats)
                    :headers {"Content-Type" "application/json"}})
    (catch Exception e (log/error "Callback error" e))))

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
  (log/info "Processing: " request)
  (swap! stats assoc-in [:processing worker-id] request)
  (swap! stats update-in [:queued] pop)
  (let [dataset (:dataset request)
        filestore (config/filestore)
        persistence (if (:no-state request) (persistence/make-no-state) (config/persistence))
        projectors (if (:no-timeline request) [] [(config/timeline filestore (select-keys request [:delivery-info]))])
        processor (processor/create dataset persistence projectors)
        zipped? (= (:format request) "zip")
        [file err] (download-file (:file request) zipped?)]
    (future (fs/cleanup-old-files filestore (* 3600 24 config/cleanup-threshold)))
    (if-not file
      (do
        (swap! stats assoc-in [:processing worker-id] nil)
        (stats-on-callback callback-chan request
                           (assoc request :error
                                  (if err err "Something went wrong downloading"))))
      (try
        (with-open [in (io/input-stream file)]
          (let [_ (log/info "Processing file: " (:file request))
                [meta features] (reader/features-from-stream in :dataset (:dataset request))
                processor (merge processor meta) ;; ugly, should move init here, but that doesnt work for the catch
                _ (dorun (consume processor features))
                processor (shutdown processor)
                statistics (:statistics processor)
                changelogs (:changelogs statistics)
                statistics (assoc-in statistics [:changelogs]
                                     (map #(config/create-url (str "api/changelogs/" %)) changelogs))
                run-stats (assoc statistics :request request)]
            (swap! stats assoc-in [:processing worker-id] nil)
            (stats-on-callback callback-chan request run-stats)))
        (catch Exception e
          (let [_ (try
                    (shutdown processor)
                    (catch Exception e
                      (log/warn e "failed to shutdown processor in exception handler")))
                error-str (if (instance? Iterable e)
                            (clojure.string/join " Next: " (map str (seq e)))
                            (str e))
                error-stats (assoc request :error error-str)]
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

(defn serve-changelog [filestore changelog]
  "Stream a changelog file identified by filename"
  (log/debug "Request for" changelog)
  (if-let [local-file (fs/get-file filestore changelog)]
    {:headers {"Content-Description"       "File Transfer"
               "Content-type"              "application/octet-stream"
               "Content-Disposition"       (str "attachment;filename=" (.getName local-file))
               "Content-Transfer-Encoding" "binary"}
     :body    local-file}
    {:status 500, :body "No such file"}))

(defn api-routes [process-chan stats]
  (defroutes api-routes
    (context "/api" []
             (GET "/info" [] (r/response {:version (slurp (clojure.java.io/resource "version"))}))
             (GET "/ping" [] (r/response {:pong (tl/local-now)}))
             (POST "/ping" [] (fn [r] (log/info "!ping pong!" (:body r)) (r/response {:pong (tl/local-now)})))
             (GET "/stats" [] (r/response @stats))
             (POST "/process" [] (partial process-request stats ProcessRequest :queued process-chan))
             (GET "/changelogs/:changelog" [changelog] (serve-changelog (config/filestore) changelog)))
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

(def process-chan (chan 1000))
(def callback-chan (chan 10))
(def stats  (atom {:processing {}
                   :queued     (PersistentQueue/EMPTY)}))

(defn init! []
  (create-workers stats callback-chan process-chan)
  (go (while true (apply callbacker (<! callback-chan)))))

(def app (-> (api-routes process-chan stats)
             (wrap-json-body {:keywords? true :bigdecimals? true})
             (wrap-json-response)
             (wrap-defaults api-defaults)
             (wrap-exception-handling)
             (routes)))
