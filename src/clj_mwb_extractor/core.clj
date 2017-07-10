(ns clj-mwb-extractor.core
  (import [java.util.zip ZipInputStream ZipEntry ZipFile]
          [java.io FileInputStream])
  (:require [clojure.xml :as xml]
            [clojure.zip :as zip]))

(defn is-doc-file? [x]
  (= "document.mwb.xml" (.getName x)))

(with-open [z (new ZipFile "resources/test.mwb")]
  (->> (enumeration-seq (.entries z))
       (filter is-doc-file?)
       (first)
       (.getInputStream z)
       xml/parse))
