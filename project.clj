(defproject lt.tokenmill/es-utils "0.1.1"
  :description "Utilities for Elasticsearch"

  :dependencies [[http-kit "2.3.0"]
                 [cheshire "5.8.0"]]

  :profiles {:dev {:dependencies  []
                   :resource-paths ["test/resources/" "resources/"]}
             :provided {:dependencies [[org.clojure/clojure "1.9.0"]
                                       [org.clojure/tools.logging "0.4.1"]]}})
