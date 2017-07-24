# clj-mwb-to-sql extractor

mysql workbench file (mwb) 파일에서 DDL을 뽑아내는 유틸리티입니다.   
CLI에서 mwb의 DDL을 exporting 하는 툴이 없어서 만들었습니다 -_-;;   
php로 만든 구현체는 있지만 인프라 구성자체에 부담이 있어서 jvm 기반으로 돌아가는 녀석이 필요했습니다.  

CI(Continous Integration)시 유용합니다. 

## Usage

```shell
java -jar exporter.jar <mwb-file> <out-file>
example : java -jar exporter.jar test.mwb test.sql

$ java -jar exporter.jar test.mwb test.sql
```


## 빌드방법 

```shell
$ lein uberjar 

## 실행 

$ java -jar target/clj-mwb-extractor-0.1.0-standalone.jar resources/test.mwb t.sql
```

## 남은작업들 

- schema 선택기능 
- mwb에 저장된 기본 데이터 export 기능 


## License

Copyright © 2017 Bohyung kim

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
