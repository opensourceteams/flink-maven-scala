配置文件 conf/flink-conf.yaml


# 指向master节点
jobmanager.rpc.address: localhost

#JAVA_HOME 环境变量设置
env.java.home: /Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home



# 设置 jobmanager heap 最大内存大小(单位MB)
jobmanager.heap.mb: 

# 设置 taskmanager heap 最大内存大小(单位MB)
taskmanager.heap.mb:



# worker节点设置,设置环境变量，内存最大大小(单位MB)  
# FLINK_TM_HEAP



配置文件 conf/slaves ,配置所有worker 的ip或host,跟hadoop一样
localhost
