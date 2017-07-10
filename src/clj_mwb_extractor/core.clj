(ns clj-mwb-extractor.core
  (import [java.util.zip ZipInputStream ZipEntry ZipFile]
          [java.io FileInputStream])
  (:require [clojure.xml :as xml]
            [clojure.zip :as zip]))

(defn is-doc-file? [x]
  (= "document.mwb.xml" (.getName x)))
(defn get-raw-data [mwb-file]
  (with-open [z (new ZipFile mwb-file)]
    (->> (enumeration-seq (.entries z))
         (filter is-doc-file?)
         (first)
         (.getInputStream z)
         xml/parse)))
(def root-node (get-raw-data "resources/test.mwb"))

root-node

(->> root-node
     :content
     first
     count)

(->> root-node
     :content
     first
     :content
     (filter #(= "physicalModels" (->> (:attrs %)
                                       :key)))
     first
     :content)


(->> root-node
     :content
     first
     (filter (fn [x]
               (= "workbench.Document" (:struct-name x)))))

