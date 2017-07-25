(ns clj-mwb-extractor.core
  (:require [clojure.xml :as xml]
            [clojure.pprint :refer [pprint]])
  (:import [java.util.zip ZipInputStream ZipEntry ZipFile]
           [java.io FileInputStream])
  (:gen-class))

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
(def table-map (atom {}))
(def column-map (atom {}))

;; dataType Map : simpleDatatypes, userDatatypes
(defn attr-key->
  "노드 리스트에서 매칭되는 [:attrs :key] 값의 textnode를 리턴"
  [list-node name]
  (let [node (->> list-node 
                  (filter #(= name (get-in % [:attrs :key]))) first)
        data-type (get-in node [:attrs :type])
        v (->> node
                   :content first)]
    (cond (= data-type "string") v
          (= data-type "int") (Integer/valueOf v)
          (= data-type "object") v)))

(defprotocol Parsable
  (parse [this]))

(deftype Column [node table-name schema-name]
  Parsable
  (parse [this]
    (let [child-node (:content node)
          id (get-in node [:attrs :id])
          name (attr-key-> child-node "name")
          table-name table-name
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
          user-type (attr-key-> child-node "userType")
          return-data {:id id
                       :name name
                       :schema-name schema-name
                       :table-name table-name
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
                       :is-not-simple-type (not (nil? simple-type))
                       :data-type (if (nil? simple-type) user-type simple-type)
                       :sql-type-def (cond
                                       (some? simple-type) (get sql-simple-type-aliases simple-type)
                                       (some? user-type) (get @sql-user-type-aliases user-type))}]
      (swap! column-map assoc id return-data)
      return-data)))

(deftype Index [node columns]
  Parsable
  (parse [this]
    (let [child-node (:content node)
          name (attr-key-> child-node "name")
          comment (attr-key-> child-node "comment")
          unique (attr-key-> child-node "unique")
          is-primary (attr-key-> child-node "isPrimary")
          index-kind (attr-key-> child-node "indexKind")
          index-type (attr-key-> child-node "indexType")
          column-data (->> child-node
                           (filter #(= "columns" (get-in % [:attrs :key]))) first
                           :content
                           (filter #(= "db.mysql.IndexColumn" (get-in % [:attrs :struct-name])))
                           (map #(let [child-node (:content %)
                                      id (attr-key-> child-node "referencedColumn")
                                      name (->> (filter (fn [x] (= id (:id x))) columns)
                                                first
                                                :name)
                                      descend (attr-key-> child-node "descend")]
                                  {:name name
                                   :descend descend})))]
      {:name name
       :comment comment
       :unique unique
       :column-data column-data
       :is-primary is-primary
       :index-type index-type
       :index-kind index-kind})))

(deftype ForeignKey [node]
  Parsable
  (parse [this]
    (let [child-node (:content node)
          name (attr-key-> child-node "name")
          delete-rule (attr-key-> child-node "deleteRule")
          update-rule (attr-key-> child-node "updateRule")
          ref-table-id (attr-key-> child-node "referencedTable")
          ref-table (get @table-map ref-table-id)
          ref-table-name (:name ref-table)
          fk-columns (->> child-node
                           (filter #(= "columns" (get-in % [:attrs :key]))) first
                           :content
                           (map #(let [column-id (first (get-in % [:content]))
                                       column (get @column-map column-id)]
                                   {:column-id column-id
                                    :column-name (:name column)
                                    :table-name (:table-name column)})))
          ref-columns (->> child-node
                           (filter #(= "referencedColumns" (get-in % [:attrs :key]))) first
                           :content
                           (map #(let [column-id (first (get-in % [:content]))
                                       column (get @column-map column-id)]

                                   {:column-id column-id
                                    :column-name (:name column)
                                    :schema-name (:schema-name column)
                                    :table-name (:table-name column)})))]
      {:name name
       :fk-columns fk-columns
       :ref-columns ref-columns
       :ref-table-id ref-table-id
       :delete-rule delete-rule
       :update-rule update-rule})))

(deftype Table [node schema-name]
  Parsable
  (parse [this]
    (let [child-node (:content node)
          name (attr-key-> child-node "name")
          id (get-in node [:attrs :id])
          table-engine (attr-key-> child-node "tableEngine")
          columns (doall (->> child-node
                       (filter #(= "columns" (get-in % [:attrs :key]))) first
                       :content
                       (map #(parse (Column. % name schema-name)))))
          indices (->> child-node
                       (filter #(= "indices" (get-in % [:attrs :key]))) first
                       :content
                       (filter #(= "db.mysql.Index" (get-in % [:attrs :struct-name])))
                       (map #(parse (Index. % columns))))
          foreign-keys (->> child-node
                            (filter #(= "foreignKeys" (get-in % [:attrs :key]))) first
                            :content
                            (filter #(= "db.mysql.ForeignKey" (get-in % [:attrs :struct-name])))
                            (map #(parse (ForeignKey. %))))
          return-data {:name name
                       :id id
                       :table-engine table-engine
                       :schema-name schema-name
                       :columns columns
                       :indices indices
                       :foreign-keys foreign-keys}]
      (swap! table-map assoc id return-data)
      return-data)))

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
                      (map #(parse (Table. % name))))]
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

(defn is-doc-file?
  "문서 정보파일인지 판단하는 predicate"
  [x]
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

(defn extract-metadata
  "document.mwb.xml에서 mysql 데이터타입 참조용 맵을 만들어낸다"
  [root-node]
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

(defn get-mwb-dsl
  "mwb를 파싱하여 export하기 좋은 자료구조로 재구성한다. "
  [file-name]
  (let [root-node (parse-xml file-name)]
    (reset! sql-user-type-aliases (extract-metadata root-node))
    (->> root-node
         Schemas.
         parse)))


(defn wrap-quot [x]
  (str \` x \`))
(defn wrap-paren [x]
  (str "(" x ")"))
(defn wrap-newline-paren [x]
  (str "(\n" x ")"))
(defn write-schema-sql [schema]
  (let [schema-name (:name schema)
        default-characterset-name (:default-characterset-name schema)
        default-collation-name (:default-collation-name schema)]
    (str
     "CREATE DATABASE IF NOT EXISTS "
     (wrap-quot schema-name) " "
     "DEFAULT CHARACTER SET " default-characterset-name " "
     "DEFAULT COLLATE " default-collation-name ";")))

;; TODO auto_increment
(defn write-column-sql [column]
  (let [column-name (wrap-quot (:name column))
        length (:length column)
        type (:sql-type-def column)
        type-def (str type
                      (cond
                        (and (> length 0) (:is-not-simple-type column)) (wrap-paren length)
                        (= "ENUM" type) (:datatype-explicit-params column)
                        :else ""))
        nullable (cond
                   (= 1 (:is-not-null column)) "NOT NULL"
                   :else "NULL")
        default-value (cond
                        (not (nil? (:default-value column))) (str "DEFAULT " (:default-value column))
                        :else "")
        comment (cond
                  (nil? (:comment column)) ""
                  :else (str "COMMENT " (:comment column)))]
    (->> ["\t" column-name type-def nullable default-value]
         (clojure.string/join " "))))

(defn write-index-columns-sql [column-data]
  (->> column-data
       (map #(str
              (wrap-quot (:name %))
              " "
              (cond
                (= 1 (:descend %)) "DESC"
                :else "ASC")))
       (clojure.string/join ",\n")))

(defn write-index-sql [indices]
  (->> indices
       (map #(cond
               (= "PRIMARY" (:index-type %)) (str "\tPRIMARY KEY "
                                                  (->> (:column-data %)
                                                       write-index-columns-sql
                                                       wrap-paren))
               :else (str "\tINDEX " (wrap-quot (:name %)))))))

(defn write-fk-columns-sql [column-data]
  (wrap-paren (->> column-data
       (map #(str
              (wrap-quot (:column-name %))))
       (clojure.string/join ","))))
(defn write-fk-references-sql [column-data]
  (->> column-data
       (map #(str
              (wrap-quot (:schema-name %)) "." (wrap-quot (:table-name %)) " "
              (wrap-paren (wrap-quot (:column-name %)))))
       (clojure.string/join ",")))


;; TODO CONSTRAINT(FK)
(defn write-fk-sql [fk]
  (let [constraint (str "\tCONSTRAINT " (wrap-quot (:name fk)) " ")
        fk-columns (:fk-columns fk)
        fk (str "\t\tFOREIGN KEY " (write-fk-columns-sql (:fk-columns fk)) "\n"
                "\t\tREFERENCES " (write-fk-references-sql (:ref-columns fk)) "\n"
                "\t\tON DELETE " (:delete-rule fk) "\n"
                "\t\tON UPDATE " (:update-rule fk) )]
    (->> [constraint fk]
       (clojure.string/join "\n"))))

(defn write-table-sql [table]
  (let [table-name (:name table)
        schema-name (:schema-name table)
        indices (write-index-sql (:indices table))
        columns (->> (:columns table)
                     (map write-column-sql))
        foreign-keys (->> (:foreign-keys table)
                          (map write-fk-sql))]
    (str
     "CREATE TABLE IF NOT EXISTS " (wrap-quot schema-name) "." (wrap-quot table-name) " "
     (wrap-newline-paren (->> [columns indices foreign-keys]
                              flatten
                              (clojure.string/join ",\n")))
     "\n ENGINE = " (:table-engine table)
     ";")))

(defn print-stdout [file]
  (->> (get-mwb-dsl file)
       (map (fn [x]
              (->> x
                   write-schema-sql
                   println)
              (->> (:tables x)
                   (map #(println (write-table-sql %)))
                   doall)))
       doall))

(defn export-sql-file [mwb-file out-file]
  (with-open [w (clojure.java.io/writer out-file)]
    (->> (get-mwb-dsl mwb-file)
         (map (fn [x]
                (->> x
                     write-schema-sql
                     (.write w))
                (.write w "\n")
                (->> (:tables x)
                     (map #(.write w (str (write-table-sql %) "\n")))
                     doall)))
         doall)))

(defn- print-usage []
  (->> ["java -jar exporter.jar <mwb-file> <out-file>"
        "example : java -jar exporter.jar test.mwb test.sql"]
       (clojure.string/join "\n")
       println))

(defn -main [& args]
  (cond
    (< 0 (count args)) (let [mwb-file (first args)
                             out-file (second args)]
                         (export-sql-file mwb-file out-file))
    :default (print-usage)))


