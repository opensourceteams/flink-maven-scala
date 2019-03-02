## 技术交流
- 微信:thinktothings
- 微博:https://weibo.com/thinktothings

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
- https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/flink-concept.md

## 执行计划
- 用Firefox 打开，显示的比较全
- https://flink.apache.org/visualizer

### Execute Plan
```aidl
{"nodes":[{"id":1,"type":"Source: Socket Stream","pact":"Data Source","contents":"Source: Socket Stream","parallelism":1},{"id":2,"type":"Flat Map","pact":"Operator","contents":"Flat Map","parallelism":1,"predecessors":[{"id":1,"ship_strategy":"FORWARD","side":"second"}]},{"id":3,"type":"Map","pact":"Operator","contents":"Map","parallelism":1,"predecessors":[{"id":2,"ship_strategy":"FORWARD","side":"second"}]},{"id":5,"type":"Window(TumblingProcessingTimeWindows(3000), ProcessingTimeTrigger, SumAggregator, PassThroughWindowFunction)","pact":"Operator","contents":"Window(TumblingProcessingTimeWindows(3000), ProcessingTimeTrigger, SumAggregator, PassThroughWindowFunction)","parallelism":1,"predecessors":[{"id":3,"ship_strategy":"HASH","side":"second"}]},{"id":6,"type":"Sink: Print to Std. Out","pact":"Data Sink","contents":"Sink: Print to Std. Out","parallelism":1,"predecessors":[{"id":5,"ship_strategy":"FORWARD","side":"second"}]}]}


```

### StreamGraph Plan
```aidl
{"nodes":[{"id":1,"type":"Source: Socket Stream","pact":"Data Source","contents":"Source: Socket Stream","parallelism":1},{"id":2,"type":"Flat Map","pact":"Operator","contents":"Flat Map","parallelism":1,"predecessors":[{"id":1,"ship_strategy":"FORWARD","side":"second"}]},{"id":3,"type":"Map","pact":"Operator","contents":"Map","parallelism":1,"predecessors":[{"id":2,"ship_strategy":"FORWARD","side":"second"}]},{"id":5,"type":"Window(TumblingProcessingTimeWindows(3000), ProcessingTimeTrigger, SumAggregator, PassThroughWindowFunction)","pact":"Operator","contents":"Window(TumblingProcessingTimeWindows(3000), ProcessingTimeTrigger, SumAggregator, PassThroughWindowFunction)","parallelism":1,"predecessors":[{"id":3,"ship_strategy":"HASH","side":"second"}]},{"id":6,"type":"Sink: Print to Std. Out","pact":"Data Sink","contents":"Sink: Print to Std. Out","parallelism":1,"predecessors":[{"id":5,"ship_strategy":"FORWARD","side":"second"}]}]}

```

### Flink 源码分析
- Flink1.7.2 local WordCount源码分析: https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/flink-local-wordCount-%E6%BA%90%E7%A0%81%E5%88%86%E6%9E%90.md
- Flink Sink 接收数据的顺序(Window发送数据顺序): https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/miniCluster/flink-sink-order.md

### Flink 时序图
- 客户端提交程序到MiniCluster(时序图): https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/images/001-%E5%AE%A2%E6%88%B7%E7%AB%AF%E6%8F%90%E4%BA%A4%E7%A8%8B%E5%BA%8F%E5%88%B0MiniCluster.png
- ExecutionGraph的构建和Execution.deploy之前(时序图): https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/images/002-ExecutionGraph%E6%9E%84%E5%BB%BA%E5%92%8C%E4%BD%9C%E4%B8%9A%E8%BF%90%E8%A1%8C.png
- Execution deploy和source数据读取(时序图): https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/images/003-execution-deploy-source%E6%95%B0%E6%8D%AE%E8%AF%BB%E5%8F%96.png
- OperatorChian计算source数据(时序图): https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/images/004_operatorChain_%E8%AE%A1%E7%AE%97source%E6%95%B0%E6%8D%AE.png
- 005-source-operation-sink源码分析: https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/images/005-source-operation-sink%E6%BA%90%E7%A0%81%E5%88%86%E6%9E%90.png