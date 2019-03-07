# Flink 源码编译

- 下载源码:
```aidl
git clone https://github.com/apache/flink.git
```

- 执行命令
```aidl
 mvn clean install -DskipTests -Dhadoop.version=2.9.2  -Dfast
```


- 输出结果

```aidl

[INFO] 
[INFO] --------------------< org.apache.flink:flink-docs >---------------------
[INFO] Building flink-docs 1.9-SNAPSHOT                               [152/152]
[INFO] --------------------------------[ jar ]---------------------------------
[INFO] 
[INFO] --- maven-clean-plugin:3.1.0:clean (default-clean) @ flink-docs ---
[INFO] Deleting /opt/resource/source/flink/flink-docs/target
[INFO] 
[INFO] --- maven-checkstyle-plugin:2.17:check (validate) @ flink-docs ---
[INFO] 
[INFO] --- maven-enforcer-plugin:3.0.0-M1:enforce (enforce-maven-version) @ flink-docs ---
[INFO] Skipping Rule Enforcement.
[INFO] 
[INFO] --- maven-enforcer-plugin:3.0.0-M1:enforce (enforce-maven) @ flink-docs ---
[INFO] Skipping Rule Enforcement.
[INFO] 
[INFO] --- maven-enforcer-plugin:3.0.0-M1:enforce (enforce-versions) @ flink-docs ---
[INFO] Skipping Rule Enforcement.
[INFO] 
[INFO] --- directory-maven-plugin:0.1:highest-basedir (directories) @ flink-docs ---
[INFO] Highest basedir set to: /opt/resource/source/flink
[INFO] 
[INFO] --- maven-remote-resources-plugin:1.5:process (process-resource-bundles) @ flink-docs ---
[INFO] 
[INFO] --- maven-resources-plugin:3.1.0:resources (default-resources) @ flink-docs ---
[INFO] Using 'UTF-8' encoding to copy filtered resources.
[INFO] skip non existing resourceDirectory /opt/resource/source/flink/flink-docs/src/main/resources
[INFO] Copying 3 resources
[INFO] 
[INFO] --- maven-compiler-plugin:3.8.0:compile (default-compile) @ flink-docs ---
[INFO] Compiling 5 source files to /opt/resource/source/flink/flink-docs/target/classes
[INFO] 
[INFO] --- maven-resources-plugin:3.1.0:testResources (default-testResources) @ flink-docs ---
[INFO] Using 'UTF-8' encoding to copy filtered resources.
[INFO] skip non existing resourceDirectory /opt/resource/source/flink/flink-docs/src/test/resources
[INFO] Copying 3 resources
[INFO] 
[INFO] --- maven-compiler-plugin:3.8.0:testCompile (default-testCompile) @ flink-docs ---
[INFO] Compiling 3 source files to /opt/resource/source/flink/flink-docs/target/test-classes
[INFO] 
[INFO] --- maven-surefire-plugin:2.22.1:test (default-test) @ flink-docs ---
[INFO] Tests are skipped.
[INFO] 
[INFO] --- maven-jar-plugin:2.4:jar (default-jar) @ flink-docs ---
[INFO] Building jar: /opt/resource/source/flink/flink-docs/target/flink-docs-1.9-SNAPSHOT.jar
[INFO] 
[INFO] --- maven-site-plugin:3.7.1:attach-descriptor (attach-descriptor) @ flink-docs ---
[INFO] Skipping because packaging 'jar' is not pom.
[INFO] 
[INFO] --- maven-shade-plugin:3.0.0:shade (shade-flink) @ flink-docs ---
[INFO] Excluding org.apache.flink:flink-annotations:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding org.apache.flink:flink-core:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding org.apache.flink:flink-metrics-core:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding org.apache.flink:flink-shaded-asm:jar:5.0.4-6.0 from the shaded jar.
[INFO] Excluding org.apache.commons:commons-lang3:jar:3.3.2 from the shaded jar.
[INFO] Excluding com.esotericsoftware.kryo:kryo:jar:2.24.0 from the shaded jar.
[INFO] Excluding com.esotericsoftware.minlog:minlog:jar:1.2 from the shaded jar.
[INFO] Excluding commons-collections:commons-collections:jar:3.2.2 from the shaded jar.
[INFO] Excluding org.apache.commons:commons-compress:jar:1.18 from the shaded jar.
[INFO] Excluding org.apache.flink:flink-shaded-guava:jar:18.0-6.0 from the shaded jar.
[INFO] Excluding org.apache.flink:flink-java:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding org.apache.flink:flink-shaded-asm-6:jar:6.2.1-6.0 from the shaded jar.
[INFO] Excluding org.apache.commons:commons-math3:jar:3.5 from the shaded jar.
[INFO] Excluding org.apache.flink:flink-runtime_2.11:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding org.apache.flink:flink-queryable-state-client-java_2.11:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding org.apache.flink:flink-hadoop-fs:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding commons-io:commons-io:jar:2.4 from the shaded jar.
[INFO] Excluding org.apache.flink:flink-shaded-jackson:jar:2.7.9-6.0 from the shaded jar.
[INFO] Excluding commons-cli:commons-cli:jar:1.3.1 from the shaded jar.
[INFO] Excluding org.javassist:javassist:jar:3.19.0-GA from the shaded jar.
[INFO] Excluding org.scala-lang:scala-library:jar:2.11.12 from the shaded jar.
[INFO] Excluding com.typesafe.akka:akka-actor_2.11:jar:2.4.20 from the shaded jar.
[INFO] Excluding com.typesafe:config:jar:1.3.0 from the shaded jar.
[INFO] Excluding org.scala-lang.modules:scala-java8-compat_2.11:jar:0.7.0 from the shaded jar.
[INFO] Excluding com.typesafe.akka:akka-remote_2.11:jar:2.4.20 from the shaded jar.
[INFO] Excluding io.netty:netty:jar:3.10.6.Final from the shaded jar.
[INFO] Excluding org.uncommons.maths:uncommons-maths:jar:1.2.2a from the shaded jar.
[INFO] Excluding com.typesafe.akka:akka-stream_2.11:jar:2.4.20 from the shaded jar.
[INFO] Excluding org.reactivestreams:reactive-streams:jar:1.0.0 from the shaded jar.
[INFO] Excluding com.typesafe:ssl-config-core_2.11:jar:0.2.1 from the shaded jar.
[INFO] Excluding org.scala-lang.modules:scala-parser-combinators_2.11:jar:1.0.4 from the shaded jar.
[INFO] Excluding com.typesafe.akka:akka-protobuf_2.11:jar:2.4.20 from the shaded jar.
[INFO] Excluding com.typesafe.akka:akka-slf4j_2.11:jar:2.4.20 from the shaded jar.
[INFO] Excluding org.clapper:grizzled-slf4j_2.11:jar:1.3.2 from the shaded jar.
[INFO] Excluding com.github.scopt:scopt_2.11:jar:3.5.0 from the shaded jar.
[INFO] Excluding org.xerial.snappy:snappy-java:jar:1.1.4 from the shaded jar.
[INFO] Excluding com.twitter:chill_2.11:jar:0.7.6 from the shaded jar.
[INFO] Excluding com.twitter:chill-java:jar:0.7.6 from the shaded jar.
[INFO] Excluding org.apache.zookeeper:zookeeper:jar:3.4.10 from the shaded jar.
[INFO] Excluding org.apache.flink:flink-shaded-curator:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding org.apache.curator:curator-recipes:jar:2.12.0 from the shaded jar.
[INFO] Excluding org.apache.curator:curator-framework:jar:2.12.0 from the shaded jar.
[INFO] Excluding org.apache.flink:flink-metrics-prometheus_2.11:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding io.prometheus:simpleclient:jar:0.3.0 from the shaded jar.
[INFO] Excluding io.prometheus:simpleclient_httpserver:jar:0.3.0 from the shaded jar.
[INFO] Excluding io.prometheus:simpleclient_common:jar:0.3.0 from the shaded jar.
[INFO] Excluding io.prometheus:simpleclient_pushgateway:jar:0.3.0 from the shaded jar.
[INFO] Excluding org.apache.flink:flink-runtime-web_2.11:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding org.apache.flink:flink-clients_2.11:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding org.apache.flink:flink-optimizer_2.11:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding org.apache.flink:flink-yarn_2.11:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding org.apache.flink:flink-shaded-hadoop2:jar:2.9.2-1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding org.apache.avro:avro:jar:1.8.2 from the shaded jar.
[INFO] Excluding org.codehaus.jackson:jackson-core-asl:jar:1.9.13 from the shaded jar.
[INFO] Excluding org.codehaus.jackson:jackson-mapper-asl:jar:1.9.13 from the shaded jar.
[INFO] Excluding com.thoughtworks.paranamer:paranamer:jar:2.7 from the shaded jar.
[INFO] Excluding org.tukaani:xz:jar:1.5 from the shaded jar.
[INFO] Excluding org.apache.hadoop:hadoop-common:jar:2.9.2 from the shaded jar.
[INFO] Excluding org.apache.hadoop:hadoop-annotations:jar:2.9.2 from the shaded jar.
[INFO] Excluding com.google.guava:guava:jar:11.0.2 from the shaded jar.
[INFO] Excluding xmlenc:xmlenc:jar:0.52 from the shaded jar.
[INFO] Excluding org.apache.httpcomponents:httpclient:jar:4.5.3 from the shaded jar.
[INFO] Excluding org.apache.httpcomponents:httpcore:jar:4.4.6 from the shaded jar.
[INFO] Excluding commons-codec:commons-codec:jar:1.10 from the shaded jar.
[INFO] Excluding commons-net:commons-net:jar:3.1 from the shaded jar.
[INFO] Excluding javax.servlet:servlet-api:jar:2.5 from the shaded jar.
[INFO] Excluding org.mortbay.jetty:jetty-sslengine:jar:6.1.26 from the shaded jar.
[INFO] Excluding commons-logging:commons-logging:jar:1.1.3 from the shaded jar.
[INFO] Excluding net.java.dev.jets3t:jets3t:jar:0.9.0 from the shaded jar.
[INFO] Excluding com.jamesmurty.utils:java-xmlbuilder:jar:0.4 from the shaded jar.
[INFO] Excluding commons-lang:commons-lang:jar:2.6 from the shaded jar.
[INFO] Excluding commons-configuration:commons-configuration:jar:1.7 from the shaded jar.
[INFO] Excluding commons-digester:commons-digester:jar:1.8.1 from the shaded jar.
[INFO] Excluding com.google.code.gson:gson:jar:2.2.4 from the shaded jar.
[INFO] Excluding org.apache.hadoop:hadoop-auth:jar:2.9.2 from the shaded jar.
[INFO] Excluding com.nimbusds:nimbus-jose-jwt:jar:4.41.1 from the shaded jar.
[INFO] Excluding com.github.stephenc.jcip:jcip-annotations:jar:1.0-1 from the shaded jar.
[INFO] Excluding net.minidev:json-smart:jar:2.3 from the shaded jar.
[INFO] Excluding net.minidev:accessors-smart:jar:1.2 from the shaded jar.
[INFO] Excluding org.apache.directory.server:apacheds-kerberos-codec:jar:2.0.0-M15 from the shaded jar.
[INFO] Excluding org.apache.directory.server:apacheds-i18n:jar:2.0.0-M15 from the shaded jar.
[INFO] Excluding org.apache.directory.api:api-asn1-api:jar:1.0.0-M20 from the shaded jar.
[INFO] Excluding org.apache.directory.api:api-util:jar:1.0.0-M20 from the shaded jar.
[INFO] Excluding com.jcraft:jsch:jar:0.1.54 from the shaded jar.
[INFO] Excluding org.apache.curator:curator-client:jar:2.7.1 from the shaded jar.
[INFO] Excluding org.apache.htrace:htrace-core4:jar:4.1.0-incubating from the shaded jar.
[INFO] Excluding org.codehaus.woodstox:stax2-api:jar:3.1.4 from the shaded jar.
[INFO] Excluding com.fasterxml.woodstox:woodstox-core:jar:5.0.3 from the shaded jar.
[INFO] Excluding commons-beanutils:commons-beanutils:jar:1.9.3 from the shaded jar.
[INFO] Excluding org.apache.hadoop:hadoop-hdfs:jar:2.9.2 from the shaded jar.
[INFO] Excluding org.apache.hadoop:hadoop-hdfs-client:jar:2.9.2 from the shaded jar.
[INFO] Excluding com.squareup.okhttp:okhttp:jar:2.7.5 from the shaded jar.
[INFO] Excluding com.squareup.okio:okio:jar:1.6.0 from the shaded jar.
[INFO] Excluding commons-daemon:commons-daemon:jar:1.0.13 from the shaded jar.
[INFO] Excluding io.netty:netty-all:jar:4.0.23.Final from the shaded jar.
[INFO] Excluding xerces:xercesImpl:jar:2.9.1 from the shaded jar.
[INFO] Excluding xml-apis:xml-apis:jar:1.3.04 from the shaded jar.
[INFO] Excluding org.fusesource.leveldbjni:leveldbjni-all:jar:1.8 from the shaded jar.
[INFO] Excluding org.apache.hadoop:hadoop-mapreduce-client-core:jar:2.9.2 from the shaded jar.
[INFO] Excluding org.apache.hadoop:hadoop-yarn-client:jar:2.9.2 from the shaded jar.
[INFO] Excluding org.apache.hadoop:hadoop-yarn-api:jar:2.9.2 from the shaded jar.
[INFO] Excluding org.apache.hadoop:hadoop-yarn-common:jar:2.9.2 from the shaded jar.
[INFO] Excluding javax.xml.bind:jaxb-api:jar:2.2.2 from the shaded jar.
[INFO] Excluding javax.xml.stream:stax-api:jar:1.0-2 from the shaded jar.
[INFO] Excluding javax.activation:activation:jar:1.1 from the shaded jar.
[INFO] Excluding com.sun.jersey:jersey-client:jar:1.9 from the shaded jar.
[INFO] Excluding org.codehaus.jackson:jackson-jaxrs:jar:1.9.13 from the shaded jar.
[INFO] Excluding org.codehaus.jackson:jackson-xc:jar:1.9.13 from the shaded jar.
[INFO] Excluding com.typesafe.akka:akka-camel_2.11:jar:2.4.20 from the shaded jar.
[INFO] Excluding org.apache.camel:camel-core:jar:2.17.7 from the shaded jar.
[INFO] Excluding org.apache.flink:flink-mesos_2.11:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding org.apache.mesos:mesos:jar:1.0.1 from the shaded jar.
[INFO] Excluding com.google.protobuf:protobuf-java:jar:2.6.1 from the shaded jar.
[INFO] Excluding com.netflix.fenzo:fenzo-core:jar:0.10.1 from the shaded jar.
[INFO] Excluding com.fasterxml.jackson.core:jackson-databind:jar:2.4.5 from the shaded jar.
[INFO] Excluding com.fasterxml.jackson.core:jackson-annotations:jar:2.4.0 from the shaded jar.
[INFO] Excluding com.fasterxml.jackson.core:jackson-core:jar:2.4.5 from the shaded jar.
[INFO] Excluding org.apache.flink:flink-shaded-netty:jar:4.1.32.Final-6.0 from the shaded jar.
[INFO] Excluding org.apache.flink:flink-shaded-jackson-module-jsonSchema:jar:2.7.9-6.0 from the shaded jar.
[INFO] Excluding org.apache.flink:flink-statebackend-rocksdb_2.11:jar:1.9-SNAPSHOT from the shaded jar.
[INFO] Excluding com.data-artisans:frocksdbjni:jar:5.17.2-artisans-1.0 from the shaded jar.
[INFO] Excluding org.slf4j:slf4j-log4j12:jar:1.7.15 from the shaded jar.
[INFO] Excluding log4j:log4j:jar:1.2.17 from the shaded jar.
[INFO] Including org.apache.flink:force-shading:jar:1.9-SNAPSHOT in the shaded jar.
[INFO] Excluding org.slf4j:slf4j-api:jar:1.7.15 from the shaded jar.
[INFO] Excluding com.google.code.findbugs:jsr305:jar:1.3.9 from the shaded jar.
[INFO] Excluding org.objenesis:objenesis:jar:2.1 from the shaded jar.
[INFO] Replacing original artifact with shaded artifact.
[INFO] Replacing /opt/resource/source/flink/flink-docs/target/flink-docs-1.9-SNAPSHOT.jar with /opt/resource/source/flink/flink-docs/target/flink-docs-1.9-SNAPSHOT-shaded.jar
[INFO] Replacing original test artifact with shaded test artifact.
[INFO] Replacing /opt/resource/source/flink/flink-docs/target/flink-docs-1.9-SNAPSHOT-tests.jar with /opt/resource/source/flink/flink-docs/target/flink-docs-1.9-SNAPSHOT-shaded-tests.jar
[INFO] Dependency-reduced POM written at: /opt/resource/source/flink/flink-docs/target/dependency-reduced-pom.xml
[INFO] 
[INFO] --- maven-surefire-plugin:2.22.1:test (integration-tests) @ flink-docs ---
[INFO] Tests are skipped.
[INFO] 
[INFO] --- maven-install-plugin:2.5.2:install (default-install) @ flink-docs ---
[INFO] Installing /opt/resource/source/flink/flink-docs/target/flink-docs-1.9-SNAPSHOT.jar to /Users/liuwen/.m2/repository/org/apache/flink/flink-docs/1.9-SNAPSHOT/flink-docs-1.9-SNAPSHOT.jar
[INFO] Installing /opt/resource/source/flink/flink-docs/target/dependency-reduced-pom.xml to /Users/liuwen/.m2/repository/org/apache/flink/flink-docs/1.9-SNAPSHOT/flink-docs-1.9-SNAPSHOT.pom
[INFO] Installing /opt/resource/source/flink/flink-docs/target/flink-docs-1.9-SNAPSHOT-tests.jar to /Users/liuwen/.m2/repository/org/apache/flink/flink-docs/1.9-SNAPSHOT/flink-docs-1.9-SNAPSHOT-tests.jar
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary:
[INFO] 
[INFO] force-shading 1.9-SNAPSHOT ......................... SUCCESS [  8.174 s]
[INFO] flink 1.9-SNAPSHOT ................................. SUCCESS [01:40 min]
[INFO] flink-annotations 1.9-SNAPSHOT ..................... SUCCESS [  3.792 s]
[INFO] flink-shaded-hadoop 1.9-SNAPSHOT ................... SUCCESS [  0.343 s]
[INFO] flink-shaded-hadoop2 2.9.2-1.9-SNAPSHOT ............ SUCCESS [ 16.875 s]
[INFO] flink-shaded-hadoop2-uber 2.9.2-1.9-SNAPSHOT ....... SUCCESS [ 27.497 s]
[INFO] flink-shaded-yarn-tests 1.9-SNAPSHOT ............... SUCCESS [01:25 min]
[INFO] flink-shaded-curator 1.9-SNAPSHOT .................. SUCCESS [  0.664 s]
[INFO] flink-metrics 1.9-SNAPSHOT ......................... SUCCESS [  0.045 s]
[INFO] flink-metrics-core 1.9-SNAPSHOT .................... SUCCESS [  0.591 s]
[INFO] flink-test-utils-parent 1.9-SNAPSHOT ............... SUCCESS [  0.058 s]
[INFO] flink-test-utils-junit 1.9-SNAPSHOT ................ SUCCESS [  0.450 s]
[INFO] flink-core 1.9-SNAPSHOT ............................ SUCCESS [ 12.466 s]
[INFO] flink-java 1.9-SNAPSHOT ............................ SUCCESS [  3.169 s]
[INFO] flink-queryable-state 1.9-SNAPSHOT ................. SUCCESS [  0.038 s]
[INFO] flink-queryable-state-client-java 1.9-SNAPSHOT ..... SUCCESS [  0.609 s]
[INFO] flink-filesystems 1.9-SNAPSHOT ..................... SUCCESS [  0.053 s]
[INFO] flink-hadoop-fs 1.9-SNAPSHOT ....................... SUCCESS [  1.212 s]
[INFO] flink-runtime 1.9-SNAPSHOT ......................... SUCCESS [ 46.890 s]
[INFO] flink-scala 1.9-SNAPSHOT ........................... SUCCESS [ 43.269 s]
[INFO] flink-mapr-fs 1.9-SNAPSHOT ......................... SUCCESS [  0.994 s]
[INFO] flink-filesystems :: flink-fs-hadoop-shaded 1.9-SNAPSHOT SUCCESS [  3.751 s]
[INFO] flink-s3-fs-base 1.9-SNAPSHOT ...................... SUCCESS [  8.347 s]
[INFO] flink-s3-fs-hadoop 1.9-SNAPSHOT .................... SUCCESS [ 11.197 s]
[INFO] flink-s3-fs-presto 1.9-SNAPSHOT .................... SUCCESS [ 15.068 s]
[INFO] flink-swift-fs-hadoop 1.9-SNAPSHOT ................. SUCCESS [ 17.295 s]
[INFO] flink-oss-fs-hadoop 1.9-SNAPSHOT ................... SUCCESS [  6.009 s]
[INFO] flink-optimizer 1.9-SNAPSHOT ....................... SUCCESS [  1.533 s]
[INFO] flink-clients 1.9-SNAPSHOT ......................... SUCCESS [  1.200 s]
[INFO] flink-streaming-java 1.9-SNAPSHOT .................. SUCCESS [  6.613 s]
[INFO] flink-test-utils 1.9-SNAPSHOT ...................... SUCCESS [  2.292 s]
[INFO] flink-runtime-web 1.9-SNAPSHOT ..................... SUCCESS [  1.258 s]
[INFO] flink-examples 1.9-SNAPSHOT ........................ SUCCESS [  0.140 s]
[INFO] flink-examples-batch 1.9-SNAPSHOT .................. SUCCESS [ 15.298 s]
[INFO] flink-connectors 1.9-SNAPSHOT ...................... SUCCESS [  0.096 s]
[INFO] flink-hadoop-compatibility 1.9-SNAPSHOT ............ SUCCESS [  6.108 s]
[INFO] flink-state-backends 1.9-SNAPSHOT .................. SUCCESS [  0.095 s]
[INFO] flink-statebackend-rocksdb 1.9-SNAPSHOT ............ SUCCESS [  1.165 s]
[INFO] flink-tests 1.9-SNAPSHOT ........................... SUCCESS [ 57.341 s]
[INFO] flink-streaming-scala 1.9-SNAPSHOT ................. SUCCESS [02:16 min]
[INFO] flink-table 1.9-SNAPSHOT ........................... SUCCESS [  0.078 s]
[INFO] flink-table-common 1.9-SNAPSHOT .................... SUCCESS [  0.950 s]
[INFO] flink-table-api-java 1.9-SNAPSHOT .................. SUCCESS [  0.390 s]
[INFO] flink-table-api-java-bridge 1.9-SNAPSHOT ........... SUCCESS [  0.700 s]
[INFO] flink-libraries 1.9-SNAPSHOT ....................... SUCCESS [  0.101 s]
[INFO] flink-cep 1.9-SNAPSHOT ............................. SUCCESS [  6.230 s]
[INFO] flink-table-planner 1.9-SNAPSHOT ................... SUCCESS [03:04 min]
[INFO] flink-orc 1.9-SNAPSHOT ............................. SUCCESS [  0.814 s]
[INFO] flink-jdbc 1.9-SNAPSHOT ............................ SUCCESS [  0.397 s]
[INFO] flink-hbase 1.9-SNAPSHOT ........................... SUCCESS [  2.701 s]
[INFO] flink-hcatalog 1.9-SNAPSHOT ........................ SUCCESS [  6.004 s]
[INFO] flink-metrics-jmx 1.9-SNAPSHOT ..................... SUCCESS [  0.547 s]
[INFO] flink-connector-kafka-base 1.9-SNAPSHOT ............ SUCCESS [  3.353 s]
[INFO] flink-connector-kafka-0.9 1.9-SNAPSHOT ............. SUCCESS [  1.599 s]
[INFO] flink-connector-kafka-0.10 1.9-SNAPSHOT ............ SUCCESS [  1.004 s]
[INFO] flink-connector-kafka-0.11 1.9-SNAPSHOT ............ SUCCESS [  0.898 s]
[INFO] flink-formats 1.9-SNAPSHOT ......................... SUCCESS [  0.064 s]
[INFO] flink-json 1.9-SNAPSHOT ............................ SUCCESS [  0.448 s]
[INFO] flink-connector-elasticsearch-base 1.9-SNAPSHOT .... SUCCESS [  1.285 s]
[INFO] flink-connector-elasticsearch 1.9-SNAPSHOT ......... SUCCESS [ 10.961 s]
[INFO] flink-connector-elasticsearch2 1.9-SNAPSHOT ........ SUCCESS [ 12.058 s]
[INFO] flink-connector-elasticsearch5 1.9-SNAPSHOT ........ SUCCESS [ 13.033 s]
[INFO] flink-connector-elasticsearch6 1.9-SNAPSHOT ........ SUCCESS [  1.293 s]
[INFO] flink-connector-rabbitmq 1.9-SNAPSHOT .............. SUCCESS [  0.308 s]
[INFO] flink-connector-twitter 1.9-SNAPSHOT ............... SUCCESS [  1.675 s]
[INFO] flink-connector-nifi 1.9-SNAPSHOT .................. SUCCESS [  0.559 s]
[INFO] flink-connector-cassandra 1.9-SNAPSHOT ............. SUCCESS [  2.779 s]
[INFO] flink-avro 1.9-SNAPSHOT ............................ SUCCESS [  2.158 s]
[INFO] flink-connector-filesystem 1.9-SNAPSHOT ............ SUCCESS [  1.058 s]
[INFO] flink-connector-kafka 1.9-SNAPSHOT ................. SUCCESS [  0.947 s]
[INFO] flink-sql-connector-elasticsearch6 1.9-SNAPSHOT .... SUCCESS [  6.832 s]
[INFO] flink-sql-connector-kafka-0.9 1.9-SNAPSHOT ......... SUCCESS [  0.301 s]
[INFO] flink-sql-connector-kafka-0.10 1.9-SNAPSHOT ........ SUCCESS [  0.425 s]
[INFO] flink-sql-connector-kafka-0.11 1.9-SNAPSHOT ........ SUCCESS [  0.591 s]
[INFO] flink-sql-connector-kafka 1.9-SNAPSHOT ............. SUCCESS [  0.709 s]
[INFO] flink-connector-kafka-0.8 1.9-SNAPSHOT ............. SUCCESS [  0.704 s]
[INFO] flink-avro-confluent-registry 1.9-SNAPSHOT ......... SUCCESS [  1.455 s]
[INFO] flink-parquet 1.9-SNAPSHOT ......................... SUCCESS [  0.842 s]
[INFO] flink-sequence-file 1.9-SNAPSHOT ................... SUCCESS [  0.383 s]
[INFO] flink-csv 1.9-SNAPSHOT ............................. SUCCESS [  0.352 s]
[INFO] flink-examples-streaming 1.9-SNAPSHOT .............. SUCCESS [ 16.885 s]
[INFO] flink-table-api-scala 1.9-SNAPSHOT ................. SUCCESS [  0.177 s]
[INFO] flink-table-api-scala-bridge 1.9-SNAPSHOT .......... SUCCESS [  0.338 s]
[INFO] flink-examples-table 1.9-SNAPSHOT .................. SUCCESS [ 11.510 s]
[INFO] flink-examples-build-helper 1.9-SNAPSHOT ........... SUCCESS [  0.087 s]
[INFO] flink-examples-streaming-twitter 1.9-SNAPSHOT ...... SUCCESS [  0.712 s]
[INFO] flink-examples-streaming-state-machine 1.9-SNAPSHOT  SUCCESS [  0.438 s]
[INFO] flink-container 1.9-SNAPSHOT ....................... SUCCESS [  0.381 s]
[INFO] flink-queryable-state-runtime 1.9-SNAPSHOT ......... SUCCESS [  0.754 s]
[INFO] flink-end-to-end-tests 1.9-SNAPSHOT ................ SUCCESS [  0.034 s]
[INFO] flink-cli-test 1.9-SNAPSHOT ........................ SUCCESS [  0.190 s]
[INFO] flink-parent-child-classloading-test-program 1.9-SNAPSHOT SUCCESS [  0.188 s]
[INFO] flink-parent-child-classloading-test-lib-package 1.9-SNAPSHOT SUCCESS [  0.125 s]
[INFO] flink-dataset-allround-test 1.9-SNAPSHOT ........... SUCCESS [  0.172 s]
[INFO] flink-datastream-allround-test 1.9-SNAPSHOT ........ SUCCESS [  1.145 s]
[INFO] flink-stream-sql-test 1.9-SNAPSHOT ................. SUCCESS [  0.254 s]
[INFO] flink-bucketing-sink-test 1.9-SNAPSHOT ............. SUCCESS [  0.500 s]
[INFO] flink-distributed-cache-via-blob 1.9-SNAPSHOT ...... SUCCESS [  0.200 s]
[INFO] flink-high-parallelism-iterations-test 1.9-SNAPSHOT  SUCCESS [  7.353 s]
[INFO] flink-stream-stateful-job-upgrade-test 1.9-SNAPSHOT  SUCCESS [  0.704 s]
[INFO] flink-queryable-state-test 1.9-SNAPSHOT ............ SUCCESS [  1.483 s]
[INFO] flink-local-recovery-and-allocation-test 1.9-SNAPSHOT SUCCESS [  0.175 s]
[INFO] flink-elasticsearch1-test 1.9-SNAPSHOT ............. SUCCESS [  2.666 s]
[INFO] flink-elasticsearch2-test 1.9-SNAPSHOT ............. SUCCESS [  4.091 s]
[INFO] flink-elasticsearch5-test 1.9-SNAPSHOT ............. SUCCESS [  4.244 s]
[INFO] flink-elasticsearch6-test 1.9-SNAPSHOT ............. SUCCESS [  2.657 s]
[INFO] flink-quickstart 1.9-SNAPSHOT ...................... SUCCESS [  0.673 s]
[INFO] flink-quickstart-java 1.9-SNAPSHOT ................. SUCCESS [  0.458 s]
[INFO] flink-quickstart-scala 1.9-SNAPSHOT ................ SUCCESS [  0.112 s]
[INFO] flink-quickstart-test 1.9-SNAPSHOT ................. SUCCESS [  0.261 s]
[INFO] flink-confluent-schema-registry 1.9-SNAPSHOT ....... SUCCESS [  1.442 s]
[INFO] flink-stream-state-ttl-test 1.9-SNAPSHOT ........... SUCCESS [  3.175 s]
[INFO] flink-sql-client-test 1.9-SNAPSHOT ................. SUCCESS [  0.633 s]
[INFO] flink-streaming-file-sink-test 1.9-SNAPSHOT ........ SUCCESS [  0.176 s]
[INFO] flink-state-evolution-test 1.9-SNAPSHOT ............ SUCCESS [  0.763 s]
[INFO] flink-e2e-test-utils 1.9-SNAPSHOT .................. SUCCESS [  6.353 s]
[INFO] flink-streaming-python 1.9-SNAPSHOT ................ SUCCESS [  7.847 s]
[INFO] flink-mesos 1.9-SNAPSHOT ........................... SUCCESS [ 21.600 s]
[INFO] flink-yarn 1.9-SNAPSHOT ............................ SUCCESS [  1.214 s]
[INFO] flink-gelly 1.9-SNAPSHOT ........................... SUCCESS [  2.036 s]
[INFO] flink-gelly-scala 1.9-SNAPSHOT ..................... SUCCESS [ 23.472 s]
[INFO] flink-gelly-examples 1.9-SNAPSHOT .................. SUCCESS [ 10.209 s]
[INFO] flink-metrics-dropwizard 1.9-SNAPSHOT .............. SUCCESS [  0.275 s]
[INFO] flink-metrics-graphite 1.9-SNAPSHOT ................ SUCCESS [  0.180 s]
[INFO] flink-metrics-influxdb 1.9-SNAPSHOT ................ SUCCESS [  0.903 s]
[INFO] flink-metrics-prometheus 1.9-SNAPSHOT .............. SUCCESS [  0.459 s]
[INFO] flink-metrics-statsd 1.9-SNAPSHOT .................. SUCCESS [  0.273 s]
[INFO] flink-metrics-datadog 1.9-SNAPSHOT ................. SUCCESS [  0.329 s]
[INFO] flink-metrics-slf4j 1.9-SNAPSHOT ................... SUCCESS [  0.230 s]
[INFO] flink-python 1.9-SNAPSHOT .......................... SUCCESS [  0.471 s]
[INFO] flink-cep-scala 1.9-SNAPSHOT ....................... SUCCESS [ 15.813 s]
[INFO] flink-ml 1.9-SNAPSHOT .............................. SUCCESS [ 46.246 s]
[INFO] flink-ml-uber 1.9-SNAPSHOT ......................... SUCCESS [  3.568 s]
[INFO] flink-table-uber 1.9-SNAPSHOT ...................... SUCCESS [  1.696 s]
[INFO] flink-sql-client 1.9-SNAPSHOT ...................... SUCCESS [  2.342 s]
[INFO] flink-scala-shell 1.9-SNAPSHOT ..................... SUCCESS [ 10.817 s]
[INFO] flink-dist 1.9-SNAPSHOT ............................ SUCCESS [ 16.444 s]
[INFO] flink-end-to-end-tests-common 1.9-SNAPSHOT ......... SUCCESS [  0.467 s]
[INFO] flink-metrics-availability-test 1.9-SNAPSHOT ....... SUCCESS [  0.263 s]
[INFO] flink-metrics-reporter-prometheus-test 1.9-SNAPSHOT  SUCCESS [  0.157 s]
[INFO] flink-heavy-deployment-stress-test 1.9-SNAPSHOT .... SUCCESS [  6.442 s]
[INFO] flink-streaming-kafka-test-base 1.9-SNAPSHOT ....... SUCCESS [  0.268 s]
[INFO] flink-streaming-kafka-test 1.9-SNAPSHOT ............ SUCCESS [  5.216 s]
[INFO] flink-streaming-kafka011-test 1.9-SNAPSHOT ......... SUCCESS [  5.284 s]
[INFO] flink-streaming-kafka010-test 1.9-SNAPSHOT ......... SUCCESS [  5.184 s]
[INFO] flink-table-runtime-blink 1.9-SNAPSHOT ............. SUCCESS [  0.529 s]
[INFO] flink-table-planner-blink 1.9-SNAPSHOT ............. SUCCESS [ 18.718 s]
[INFO] flink-contrib 1.9-SNAPSHOT ......................... SUCCESS [  0.030 s]
[INFO] flink-connector-wikiedits 1.9-SNAPSHOT ............. SUCCESS [  0.344 s]
[INFO] flink-yarn-tests 1.9-SNAPSHOT ...................... SUCCESS [ 37.961 s]
[INFO] flink-fs-tests 1.9-SNAPSHOT ........................ SUCCESS [  0.404 s]
[INFO] flink-docs 1.9-SNAPSHOT ............................ SUCCESS [  0.774 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  20:39 min
[INFO] Finished at: 2019-03-07T12:06:37+08:00
[INFO] ------------------------------------------------------------------------

```