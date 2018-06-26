(ns es-utils-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [es-utils :as es]))

(deftest main-test
  (is (string? (slurp (es/as-input-stream "index-conf.json"))))
  (is (string? (slurp (es/as-input-stream (io/file "project.clj")))))

  (testing "if index name is correct"
    (is (= "docs_v4" (es/data->index-name "test-docs.json")))
    (is (= "docs_v4" (es/data->index-name (io/file "test/resources/test-docs.json"))))))
