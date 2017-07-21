(ns clj-mwb-extractor.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clj-mwb-extractor.core :refer :all]))
(deftest test0
  (testing "테이블의 foreignkey데이터 확인"
    (let [r (doall (get-mwb-dsl "resources/test.mwb"))
          schema (->> r first)
          tables (->> r first :tables)
          table (->> tables
                     second)]
      (println (write-schema-sql schema))
      (println (write-table-sql table))
      (is (= 1 1)))))
