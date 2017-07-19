(ns clj-mwb-extractor.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clj-mwb-extractor.core :refer :all]))

(deftest test0
  (testing "테이블의 foreignkey데이터 확인"
    (let [r (doall (get-mwb-dsl "resources/test.mwb"))]
      (pprint (->> r
                   first
                   :tables
                   (filter #(= "hostel_reserv" (get % :name)))))
      (is (= 1 1))))
  (testing "test2"
    (is (= 1 1))))
