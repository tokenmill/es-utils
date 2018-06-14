(ns es-utils
  (:require [clojure.tools.logging :as log]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [cheshire.core :as cheshire]
            [org.httpkit.client :as http]))

(defn es-ready? [es-host]
  (loop [{:keys [status]} @(http/request {:url es-host})
         idx 0]
    (if (= 200 status)
      true
      (if (< 10 idx)
        false
        (do
          (log/warnf "Waiting for Elasticsearch on '%s'." es-host)
          (Thread/sleep (* 1000))
          (recur @(http/request {:url es-host}) (inc idx)))))))

(defn put-alias [es-host index-name alias]
  @(http/request
     {:url     (format "%s/%s/_alias/%s" es-host index-name alias)
      :method  :put
      :headers {"Content-Type" "application/json"}}))

(defn create-index [es-host index-name index-config]
  @(http/request
     {:url     (format "%s/%s" es-host index-name)
      :method  :put
      :headers {"Content-Type" "application/json"}
      :body    (io/input-stream (io/resource index-config))}))

(defn data->index-name [data-file]
  (-> data-file (io/resource) (io/reader) (line-seq) (first) (cheshire/decode true) :index :_index))

(defn import-test-data-to-es [es-host es-index-name input-file]
  (let [resp (-> @(http/request
                    {:url     (format "%s/_bulk" es-host)
                     :method  :post
                     :headers {"Content-Type" "application/x-ndjson"}
                     :body    (io/input-stream (io/resource input-file))})
                 :body
                 (cheshire/decode))]
    (log/infof "Data upload from file '%s' had errors '%s' for items count '%s'"
               input-file (get resp "errors") (count (get resp "items"))))
  (let [resp @(http/request
                {:url    (format "%s/%s/_refresh" es-host es-index-name)
                 :method :post})]
    (log/infof "Refresh for index '%s' resulted in status '%s'" es-index-name (:status resp))))

(defn exists? [es-host index-name]
  (when (= 200 (-> @(http/request {:url (format "%s/%s" es-host index-name) :method :head}) :status))
    true))

(defn actual-index-name [es-host index-name]
  (-> @(http/request {:url (format "%s/%s" es-host index-name)
                      :method :get})
      :body
      (cheshire/decode true)
      (vals)
      (first)
      :settings
      :index
      :provided_name))

(defn configure-index [es-host {:keys [alias conf data] :as index-conf}]
  (try
    (let [index-name (if data (data->index-name data) alias)]
      (when (exists? es-host index-name)
        (let [physical-index-name (actual-index-name es-host index-name)]
          (log/infof "Index '%s' deleted '%s'"
                     physical-index-name
                     (-> @(http/request {:url    (format "%s/%s" es-host physical-index-name)
                                         :method :delete})
                         :body
                         (cheshire/decode true)))))
      (log/infof "Index '%s' with conf '%s' created with status '%s'"
                 index-name index-conf
                 (:status (create-index es-host index-name conf)))
      (when-not (= index-name alias)
        (log/infof "Alias '%s' was put on index '%s' with status '%s'" alias index-name
                   (:status (put-alias es-host index-name alias)))))
    (catch Exception e
      (.printStackTrace e))))

(defn prepare-indices [env es-host es-index-confs]
  (doseq [[k v] es-index-confs]
    (configure-index es-host (assoc v :alias (or (get env k) (get v :alias))))))

(defn load-test-data [env es-host index-confs]
  (doseq [[k {:keys [alias data]}] index-confs]
    (when data
      (import-test-data-to-es es-host (or (get env k) alias) data))))

(defn delete-all-docs [es-host es-index-confs]
  (let [indices (->> es-index-confs (map (fn [[_ {:keys [alias]}]] alias)) (str/join ","))
        resp @(http/request
                {:url          (format "%s/%s/_delete_by_query?refresh&wait_for_completion=true"
                                       es-host indices)
                 :method       :post
                 :body         (cheshire/encode {:query {:match_all {}}})
                 :headers {"Content-Type" "application/json; charset=UTF-8"}})]
    (if (or (seq (:failures resp)) (not= 200 (:status resp)))
      (log/errorf "Failed to delete all docs: %s" resp)
      (log/infof "Deleted all documents from indices '%s'." indices))))

(defn export-docs-for-bulk-via-http
  "Example: (export-docs-for-bulk \"http://127.0.0.1:9200\" \"index\" {:query {:match_all {}} :size 1000} \"output.json\")"
  [es-host index-name query out-file]
  (let [op (fn [hit] {"index" {"_index" (:_index hit) "_type" (:_type hit) "_id" (:_id hit)}})]
    (try
      (let [hits (-> @(http/request {:url     (format "%s/%s/_search" es-host index-name)
                                     :method  :post
                                     :headers {"Content-Type" "application/json; charset=UTF-8"}
                                     :body    (cheshire/encode query)})
                     :body (cheshire/decode true) :hits :hits)
            ops (reduce #(conj %1 (op %2) (:_source %2)) [] hits)]
        (doseq [line ops]
          (spit out-file (str (cheshire/encode line) "\n") :append true)))
      (catch Exception e
        (log/errorf "ES export failed '%s' with exception %s" query e)))))
