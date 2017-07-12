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

(deftype Schemas [root-node]
  Parsable
  (parse [this]
    (let [child-node (->> root-node
                          :content first
                          :content (filter #(= "physicalModels" (get-in % [:attrs :key]))) first
                          :content first
                          :content first)
          schemas (->> child-node
                       :content (filter #(= "schemata" (get-in % [:attrs :key]))) first
                       :content (filter #(= "db.mysql.Schema" (get-in % [:attrs :struct-name]))))
          data-type-map (->> child-node
                             :content (filter #(= "simpleDatatypes" (get-in % [:attrs :key]))) first
                             :content
                             (map (fn [x]
                                    (let [id (get-in x [:content 0])]
                                      {:id id
                                       :sql-definition -1}))))
          user-data-types (->> child-node
                               :content (filter #(= "userDatatypes" (get-in % [:attrs :key]))) first
                               :content
                               (map (fn [x]
                                      (let [id (get-in x [:attrs :id])
                                            sql-definition (attr-key-> (:content x) "sqlDefinition")
                                            actual-type (attr-key-> (:content x) "actualType")]
                                        {:id id
                                         :sql-definition sql-definition
                                         :actual-type actual-type}))))]
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
          id (get-in node [:attrs :id])
          name (attr-key-> child-node "name")
          auto-increment (attr-key-> child-node "autoIncrement")
          character-set-name (attr-key-> child-node "characterSetName")
          collation-name (attr-key-> child-node "collationName")
          datatype-explicit-params (attr-key-> child-node "datatypeExplicitParams")
          default-value (attr-key-> child-node "defaultValue")
          default-value-is-null (attr-key-> child-node "defaultValueIsNull")
          is-not-null (attr-key-> child-node "isNotNull")
          length (attr-key-> child-node "length")
          precision (attr-key-> child-node "precision")
          scale (attr-key-> child-node "scale")
          comment (attr-key-> child-node "comment")
          owner (attr-key-> child-node "owner")

          simple-type (attr-key-> child-node "simpleType")
          user-type (attr-key-> child-node "userType")]
      {:id id
       :name name
       :auto-increment auto-increment
       :character-set-name character-set-name
       :collation-name collation-name
       :datatype-explicit-params datatype-explicit-params
       :default-value default-value
       :default-value-is-null default-value-is-null
       :is-not-null is-not-null
       :length length
       :precision precision
       :scale scale
       :comment comment
       :owner owner
       :data-type (if (nil? simple-type) user-type simple-type)})))

;; dataType Map : simpleDatatypes, userDatatypes
(defn attr-key->
  "노드 리스트에서 매칭되는 [:attrs :key] 값의 textnode를 리턴"
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
