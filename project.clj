(defproject clj-mwb-extractor "0.1.0"
  :description "Clojure Mysql Workbench Exporter"
  :url "http://github.com/dsdstudio/clj-mwb-exporter"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha17"]]
  :main clj-mwb-extractor.core
  :aot [clj-mwb-extractor.core]
  :jar true
  :plugins [[cider/cider-nrepl "0.15.0-SNAPSHOT"]])
