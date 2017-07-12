(ns clj-mwb-extractor.core
  (import [java.util.zip ZipInputStream ZipEntry ZipFile]
          [java.io FileInputStream])
  (:require [clojure.xml :as xml]))

(defn is-doc-file? [x]
  (= "document.mwb.xml" (.getName x)))

(defn get-raw-data [mwb-file]
  (with-open [z (new ZipFile mwb-file)]
    (->> (enumeration-seq (.entries z)) ;; enumeration을 시퀀스로 변환 
         (filter is-doc-file?) ;; 그중 doc파일만 추림 
         first ;; 한개나왔을거니 첫번째요소 리턴 
         (.getInputStream z) ;; ZipEntry에서 stream을 뽑아냄 
         xml/parse))) ;; xml로 파싱 

(def root-node (get-raw-data "resources/test.mwb"))

(defprotocol Parsable
  (parse [this]))

(deftype Schemas [node]
  Parsable
  (parse [this]
    (let [schemas (->> root-node
                       :content first
                       :content (filter #(= "physicalModels" (get-in % [:attrs :key]))) first
                       :content first
                       :content first
                       :content (filter #(= "schemata" (get-in % [:attrs :key]))) first
                       :content (filter #(= "db.mysql.Schema" (get-in % [:attrs :struct-name]))))]
      (map #(parse (Schema. %)) schemas))))

(deftype Schema [node]
  Parsable
  (parse [this]
    (let [child-node (:content node)
          name (attr-key-> child-node "name")
          default-characterset-name (attr-key-> child-node "defaultCharacterSetName")
          default-collation-name (attr-key-> child-node "defaultCollationName")
          tables (->> child-node
                      (filter #(= "tables" (get-in % [:attrs :key]))) first
                      :content
                      (map #(parse (Table. %))))]
      {:name name
       :default-characterset-name default-characterset-name
       :default-collation-name default-collation-name
       :tables tables})))
          

(deftype Table [node]
  Parsable
  (parse [this]
    (let [child-node (:content node)
          name (attr-key-> child-node "name")
          columns (->> child-node
                       (filter #(= "columns" (get-in % [:attrs :key]))) first
                       :content
                       (map #(parse (Column. %))))]
      ;; TODO indexes
      ;; TODO foreignkeys
      {:name name
       :columns columns})))
(deftype Column [node]
  Parsable
  (parse [this]
    (let [child-node (:content node)
          name (attr-key-> child-node "name")
          ]
      ;; TODO datatype length
      ;; TODO PRIMARY KEY
      ;; TODO UNIQUE
      ;; TODO BIN
      ;; TODO UN
      ;; TODO ZF
      ;; TODO AutoIncrement
      ;; TODO DEFAULT VALUE
      
      {:name name})))

(defn attr-key->
  "노드 리스트에서 매칭되는 [:attrs :key] 값의 textnode를 리턴한다."
  [list-node name]
  (let [node (->> list-node 
                  (filter #(= name (get-in % [:attrs :key]))) first)
        data-type (get-in node [:attrs :type])
        v (->> node
                   :content first)]
    ;; TODO object datatype에 대한 처리 필요
    (cond (= data-type "string") v
          (= data-type "int") (Integer/valueOf v)
          (= data-type "object") v)))
(clojure.pprint/pprint (parse (Schemas. root-node)))

;; Schemas - Schema - tables - table - indexes, columns, foreignkeys
(comment
  tables - columns
         - indicies
         - Foreignkeys   
  )
