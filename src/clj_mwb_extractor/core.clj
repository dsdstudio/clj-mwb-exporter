(ns clj-mwb-extractor.core
  (import [java.util.zip ZipInputStream ZipEntry ZipFile]
          [java.io FileInputStream])
  (:require [clojure.xml :as xml]))



(defn is-doc-file? [x]
  (= "document.mwb.xml" (.getName x)))
(defn get-raw-data [mwb-file]
  (with-open [z (new ZipFile mwb-file)]
    (->> (enumeration-seq (.entries z))
         (filter is-doc-file?)
         first
         (.getInputStream z)
         xml/parse)))
(def root-node (get-raw-data "resources/test.mwb"))

root-node

(->> root-node
     :content first
     :content (filter #(= "physicalModels" (get-in % [:attrs :key]))) first
     :content first
     :content first
     :content (filter #(= "schemata" (get-in % [:attrs :key]))) first
     :content (filter #(= "db.mysql.Schema" (get-in % [:attrs :struct-name]))) first
     :content (filter #(= "tables" (get-in % [:attrs :key]))))

;; TODO 테이블 정보 뽑기 
(comment
  tables - columns
         - indicies
         - Foreignkeys   
  )
