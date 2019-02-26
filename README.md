###  查看jar中文件列表
```aidl
jar tvf test.jar 

```
### 执行计划图
- 地址:https://flink.apache.org/visualizer

```aidl
      //执行计划
      //println(env.getExecutionPlan)
      //StreamGraph
     //println(env.getStreamGraph.getStreamingPlanAsJSON)
```

### 运行 jar 到 Flink 集群
```aidl
 flink run -c  com.opensourceteams.module.bigdata.flink.example.stream.worldcount.nc.SocketWindowWordCount    ./flink-maven-scala-2-0.0.1.jar  

```

### 创建flink java 项目
```aidl
mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-java     \
      -DarchetypeVersion=1.7.1
      -DgroupId=com.opensourceteams \
      -DartifactId=flink-maven-java \
      -Dversion=0.0.1 \
      -Dpackage=com.opensourceteams.module.bigdata.flink  \
      -DinteractiveMode=false

```

### 创建flink scala项目
```aidl
mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-scala     \
      -DarchetypeVersion=1.7.1
      -DgroupId=com.opensourceteams \
      -DartifactId=flink-maven-scala-2 \
      -Dversion=0.0.1 \
      -Dpackage=com.opensourceteams.module.bigdata.flink  \
      -DinteractiveMode=false

```


### maven 运行某个类
```aidl
mvn exec:java -Dexec.mainClass=wikiedits.WikipediaAnalysis

```


### Flink 源码debug方法
- https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/flink-debug.md


### 运行 jar 到 Flink 集群
```aidl
 flink run -c  com.opensourceteams.module.bigdata.flink.example.stream.worldcount.nc.SocketWindowWordCount    ./flink-maven-scala-2-0.0.1.jar  

```

### scala 版Flink WordCount单词统计 
- https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/wordCount-scala.md


### Flink MiniCluster 作业提交
- https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/miniCluster/MiniCluster-job-submit.md



### Flink 名词术语
- https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/miniCluster/flink-concept.md