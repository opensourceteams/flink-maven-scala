mvn clean package -DskipTests
scp -r  target/flink-maven-scala-2-0.0.1.jar standalone.com:/home/liuwen/temp/jar/