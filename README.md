###  查看jar中文件列表
```aidl
jar tvf test.jar 

```


### 运行 jar 到 Flink 集群
```aidl
 flink run -c  com.opensourceteams.module.bigdata.flink.example.stream.worldcount.nc.SocketWindowWordCount    ./flink-maven-scala-2-0.0.1.jar  

```


### Flink 源码debug方法
- https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/flink-debug.md

### Flink MiniCluster 作业提交
- https://github.com/opensourceteams/fink-maven-scala-2/blob/master/md/miniCluster/MiniCluster-job-submit.md