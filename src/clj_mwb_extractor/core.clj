(ns clj-mwb-extractor.core
  (import [java.util.zip ZipInputStream ZipEntry ZipFile]
          [java.io FileInputStream])
  (:require [clojure.xml :as xml]
            [clojure.pprint :refer [pprint]]))

(defonce sql-simple-type-aliases
  {"com.mysql.rdbms.mysql.datatype.tinyint" "TINYINT"
   "com.mysql.rdbms.mysql.datatype.smallint" "SMALLINT"
   "com.mysql.rdbms.mysql.datatype.mediumint" "MEDIUMINT"
   "com.mysql.rdbms.mysql.datatype.int" "INT"
   "com.mysql.rdbms.mysql.datatype.bigint" "BIGINT"
   "com.mysql.rdbms.mysql.datatype.float" "FLOAT"
   "com.mysql.rdbms.mysql.datatype.double" "DOUBLE"
   "com.mysql.rdbms.mysql.datatype.decimal" "DECIMAL"
   "com.mysql.rdbms.mysql.datatype.char" "CHAR"
   "com.mysql.rdbms.mysql.datatype.varchar" "VARCHAR"
   "com.mysql.rdbms.mysql.datatype.binary" "BINARY"
   "com.mysql.rdbms.mysql.datatype.varbinary" "VARBINARY"
   "com.mysql.rdbms.mysql.datatype.tinytext" "TINYTEXT"
   "com.mysql.rdbms.mysql.datatype.text" "TEXT"
   "com.mysql.rdbms.mysql.datatype.mediumtext" "MEDIUMTEXT"
   "com.mysql.rdbms.mysql.datatype.longtext" "LONGTEXT"
   "com.mysql.rdbms.mysql.datatype.tinyblob" "TINYBLOB"
   "com.mysql.rdbms.mysql.datatype.blob" "BLOB"
   "com.mysql.rdbms.mysql.datatype.mediumblob" "MEDIUMBLOB"
   "com.mysql.rdbms.mysql.datatype.longblob" "LONGBLOB"
   "com.mysql.rdbms.mysql.datatype.datetime" "DATETIME"
   "com.mysql.rdbms.mysql.datatype.datetime_f" "DATETIME"
   "com.mysql.rdbms.mysql.datatype.date" "DATE"
   "com.mysql.rdbms.mysql.datatype.time" "TIME"
   "com.mysql.rdbms.mysql.datatype.time_f" "TIME"
   "com.mysql.rdbms.mysql.datatype.year" "YEAR"
   "com.mysql.rdbms.mysql.datatype.timestamp" "TIMESTAMP"
   "com.mysql.rdbms.mysql.datatype.timestamp_f" "TIMESTAMP"
   "com.mysql.rdbms.mysql.datatype.geometry" "GEOMETRY"
   "com.mysql.rdbms.mysql.datatype.point" "POINT"
   "com.mysql.rdbms.mysql.datatype.real" "REAL"
   "com.mysql.rdbms.mysql.datatype.nchar" "NATIONAL CHAR"
   "com.mysql.rdbms.mysql.datatype.nvarchar" "NATIONAL VARCHAR"
   "com.mysql.rdbms.mysql.datatype.linestring" "LINESTRING"
   "com.mysql.rdbms.mysql.datatype.polygon" "POLYGON"
   "com.mysql.rdbms.mysql.datatype.geometrycollection" "GEOMETRYCOLLECTION"
   "com.mysql.rdbms.mysql.datatype.multipoint" "MULTIPOINT"
   "com.mysql.rdbms.mysql.datatype.multilinestring" "MULTILINESTRING"
   "com.mysql.rdbms.mysql.datatype.multipolygon" "MULTIPOLYGON"
   "com.mysql.rdbms.mysql.datatype.bit" "BIT"
   "com.mysql.rdbms.mysql.datatype.enum" "ENUM"
   "com.mysql.rdbms.mysql.datatype.set" "SET"})
(def sql-user-type-aliases (atom {}))
  
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

(defprotocol Parsable
  (parse [this]))
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
       :data-type (if (nil? simple-type) user-type simple-type)
       :sql-type-def (cond
                       (some? simple-type) (get sql-simple-type-aliases simple-type)
                       (some? user-type) (get @sql-user-type-aliases user-type))})))

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
                       :content (filter #(= "db.mysql.Schema" (get-in % [:attrs :struct-name]))))]
      (map #(parse (Schema. %)) schemas))))


(defn is-doc-file? [x]
  (= "document.mwb.xml" (.getName x)))

(defn parse-xml
  "mwb로 부터 sql export를 위한 EDN DSL을 얻어낸다."
  [mwb-file]
  (with-open [z (new ZipFile mwb-file)]
    (->> (enumeration-seq (.entries z)) ;; enumeration을 시퀀스로 변환 
         (filter is-doc-file?) ;; 그중 doc파일만 추림 
         first ;; 한개나왔을거니 첫번째요소 리턴 
         (.getInputStream z) ;; ZipEntry에서 stream을 뽑아냄 
         xml/parse))) ;; xml로 파싱 

(defn extract-metadata [root-node]
  (->> root-node
       :content first
       :content (filter #(= "physicalModels" (get-in % [:attrs :key]))) first
       :content first
       :content first
       :content (filter #(= "userDatatypes" (get-in % [:attrs :key]))) first
       :content
       (reduce (fn [m x]
                 (let [id (get-in x [:attrs :id])
                       sql-definition (attr-key-> (:content x) "sqlDefinition")
                       actual-type (attr-key-> (:content x) "actualType")]
                   (assoc m id sql-definition))) {})))

(defn get-mwb-dsl [file-name]
  (let [root-node (parse-xml file-name)]
    (reset! sql-user-type-aliases (extract-metadata root-node))
    (->> root-node 
         Schemas.
         parse)))

(pprint (get-mwb-dsl "resources/test.mwb"))

(pprint @sql-user-type-aliases)

;; Schemas - Schema - tables - table - indexes, columns, foreignkeys
(comment
  tables - columns
         - indicies
         - Foreignkeys
  )
