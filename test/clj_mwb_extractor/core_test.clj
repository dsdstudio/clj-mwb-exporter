(ns clj-mwb-extractor.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clj-mwb-extractor.core :refer :all]))

(defn write-sql [table]
  (let [table-name (:name table)]
    (str "CREATE TABLE " (:schema-name table) "." table-name)))

(deftest test0
  (testing "테이블의 foreignkey데이터 확인"
    (let [r (doall (get-mwb-dsl "resources/test.mwb"))
          table (->> r
                     first
                     :tables
                     first)]
      (pprint (keys table))
      (pprint (write-sql table))
      (is (= 1 1)))
  (testing "test2"
    (is (= 1 1)))))
