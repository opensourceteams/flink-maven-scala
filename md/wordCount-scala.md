# Flink worldCount 体验Scala版本

## 源码
- github: https://github.com/opensourceteams/fink-maven-scala


## 描述
- nc命令发送数据，Flink 接收数据，并对接数据统计单词总数
- 一共分为四步
 - 按行打印输入数据
 - 拆分单词
 - 拆分单词为(Key,value)
 - 相同单词进行统计个数

## pom.xml
```

<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
	<modelVersion>4.0.0</modelVersion>

	<groupId>com.opensourceteams</groupId>
	<artifactId>flink-maven-scala</artifactId>
	<version>0.0.1</version>
	<packaging>jar</packaging>

	<name>flink-maven-scala</name>
	<url>http://www.myorganization.org</url>

	<properties>
		<project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
		<flink.version>1.7.1</flink.version>
		<java.version>1.8</java.version>
		<scala.binary.version>2.11</scala.binary.version>
		<scala.version>2.11.12</scala.version>
		<maven.compiler.source>${java.version}</maven.compiler.source>
		<maven.compiler.target>${java.version}</maven.compiler.target>
	</properties>

	<repositories>
		<repository>
			<id>apache.snapshots</id>
			<name>Apache Development Snapshot Repository</name>
			<url>https://repository.apache.org/content/repositories/snapshots/</url>
			<releases>
				<enabled>false</enabled>
			</releases>
			<snapshots>
				<enabled>true</enabled>
			</snapshots>
		</repository>
	</repositories>

	<dependencies>
		<!-- Apache Flink dependencies -->
		<!-- These dependencies are provided, because they should not be packaged into the JAR file. -->

		<!-- Scala Library, provided by Flink as well. -->
		<dependency>
			<groupId>org.scala-lang</groupId>
			<artifactId>scala-library</artifactId>
			<version>${scala.version}</version>
			<scope>provided</scope>
		</dependency>


		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-scala_${scala.binary.version}</artifactId>
			<version>${flink.version}</version>
			<scope>provided</scope>
		</dependency>
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-streaming-scala_${scala.binary.version}</artifactId>
			<version>${flink.version}</version>
			<scope>provided</scope>
		</dependency>




	<!--	<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-clients_2.11</artifactId>
			<version>${flink.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-connector-wikiedits_2.11</artifactId>
			<version>${flink.version}</version>
		</dependency>
-->



		<!-- Add connector dependencies here. They must be in the default scope (compile). -->

		<!-- Example:

		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-connector-kafka-0.10_${scala.binary.version}</artifactId>
			<version>${flink.version}</version>
		</dependency>
		-->

		<!-- Add logging framework, to produce console output when running in the IDE. -->
		<!-- These dependencies are excluded from the application JAR by default. -->
		<dependency>
			<groupId>org.slf4j</groupId>
			<artifactId>slf4j-log4j12</artifactId>
			<version>1.7.7</version>
			<scope>runtime</scope>
		</dependency>
		<dependency>
			<groupId>log4j</groupId>
			<artifactId>log4j</artifactId>
			<version>1.2.17</version>
			<scope>runtime</scope>
		</dependency>
	</dependencies>

	<build>
		<!--scala待编译的文件目录-->
		<sourceDirectory>src/main/scala</sourceDirectory>
		<testSourceDirectory>src/test/scala</testSourceDirectory>


		<resources>
			<resource>
				<directory>${project.basedir}/src/main/resources</directory>
				<includes>
					<include>*.properties</include>
					<include>*.xml</include>
				</includes>
			</resource>
		</resources>
		<plugins>
			<!-- We use the maven-shade plugin to create a fat jar that contains all necessary dependencies. -->
			<!-- Change the value of <mainClass>...</mainClass> if your program entry point changes. -->
			<plugin>
				<groupId>org.apache.maven.plugins</groupId>
				<artifactId>maven-shade-plugin</artifactId>
				<version>3.0.0</version>
				<executions>
					<!-- Run shade goal on package phase -->
					<execution>
						<phase>package</phase>
						<goals>
							<goal>shade</goal>
						</goals>
						<configuration>
							<artifactSet>
								<excludes>
									<exclude>org.apache.flink:force-shading</exclude>
									<exclude>com.google.code.findbugs:jsr305</exclude>
									<exclude>org.slf4j:*</exclude>
									<exclude>log4j:*</exclude>
								</excludes>
							</artifactSet>
							<filters>
								<filter>
									<!-- Do not copy the signatures in the META-INF folder.
									Otherwise, this might cause SecurityExceptions when using the JAR. -->
									<artifact>*:*</artifact>
									<excludes>
										<exclude>META-INF/*.SF</exclude>
										<exclude>META-INF/*.DSA</exclude>
										<exclude>META-INF/*.RSA</exclude>
									</excludes>
								</filter>
							</filters>
							<transformers>
								<transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
									<mainClass>org.myorg.quickstart.StreamingJob</mainClass>
								</transformer>
							</transformers>
						</configuration>
					</execution>
				</executions>
			</plugin>

			<!-- Java Compiler -->
			<plugin>
				<groupId>org.apache.maven.plugins</groupId>
				<artifactId>maven-compiler-plugin</artifactId>
				<version>3.1</version>
				<configuration>
					<source>1.8</source>
					<target>1.8</target>
				</configuration>
			</plugin>

			<!-- Scala Compiler -->
			<plugin>
				<groupId>net.alchim31.maven</groupId>
				<artifactId>scala-maven-plugin</artifactId>
				<version>3.2.2</version>
				<executions>
					<execution>
						<goals>
							<goal>compile</goal>
							<goal>testCompile</goal>
						</goals>
					</execution>
				</executions>
			</plugin>
		</plugins>

	</build>

	<!-- This profile helps to make things run out of the box in IntelliJ -->
	<!-- Its adds Flink's core classes to the runtime class path. -->
	<!-- Otherwise they are missing in IntelliJ, because the dependency is 'provided' -->
	<profiles>
		<profile>
			<id>add-dependencies-for-IDEA</id>

			<activation>
				<property>
					<name>idea.version</name>
				</property>
			</activation>

			<dependencies>
				<dependency>
					<groupId>org.apache.flink</groupId>
					<artifactId>flink-scala_${scala.binary.version}</artifactId>
					<version>${flink.version}</version>
					<scope>compile</scope>
				</dependency>
				<dependency>
					<groupId>org.apache.flink</groupId>
					<artifactId>flink-streaming-scala_${scala.binary.version}</artifactId>
					<version>${flink.version}</version>
					<scope>compile</scope>
				</dependency>
				<dependency>
					<groupId>org.scala-lang</groupId>
					<artifactId>scala-library</artifactId>
					<version>${scala.version}</version>
					<scope>compile</scope>
				</dependency>


			</dependencies>
		</profile>
	</profiles>

</project>


```
## 按行打印输入数据

### nc 命令，输入数据
- 在本地端口1234开启服务
```
nc -l 1234
```

### 一、按行打印输入数据Java
```
package com.module.flink.example.worldcount.stream.n_001_source_nc.n_001_打印输入数据

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


object Run {

  def main(args: Array[String]): Unit = {

    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')

    text.print().setParallelism(1)

    env.execute("打印输入数据")

  }

}

```

### 二、拆分单词
```
package com.module.flink.example.worldcount.stream.n_001_source_nc.n_002_折分单词

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object Run {

  def main(args: Array[String]): Unit = {

    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')


    import org.apache.flink.streaming.api.scala._
    val textFlatMap = text.flatMap { w => w.split("\\s") }

    textFlatMap.print().setParallelism(1)

    env.execute("打印输入数据")

  }

}

```
### 三、拆分单词为(Key,value)
```
package com.module.flink.example.worldcount.stream.n_001_source_nc.n_003_拆分单词为KeyValue

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object Run {

  def main(args: Array[String]): Unit = {

    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')


    import org.apache.flink.streaming.api.scala._
    val textResult = text.flatMap( w => w.split("\\s") ).map( w => (w,1))

    textResult.print().setParallelism(1)

    env.execute("打印输入数据")

  }

}

```
### 四、相同单词进行统计个数
- Run

```
package com.module.flink.example.worldcount.stream.n_001_source_nc.n_004_相同单词统计

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object Run {

  def main(args: Array[String]): Unit = {

    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')


    import org.apache.flink.streaming.api.scala._
    val textResult = text.flatMap( w => w.split("\\s") ).map( w => WordWithCount(w,1))
      .keyBy("word").reduce((a,b) => WordWithCount(a.word ,a.count + b.count )  )

    textResult.print().setParallelism(1)

    env.execute("打印输入数据")

  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}

```

end