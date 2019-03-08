# Flink 1.7.2 DataStream operator

## 源码
- https://github.com/opensourceteams/flink-maven-scala

### map
- 处理所有元素
- 输入数据
```
a
```
- 程序
```
```
- 输出数据
 
```
    a
```

### map
- 处理所有元素
- 输入数据
 
    ```
    你好
    发送数据
    ```
- 程序
 
    ```

    package com.opensourceteams.module.bigdata.flink.example.stream.operator.map
    
    import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
    
    import org.apache.flink.streaming.api.scala._
    
    /**
      * nc -lk 1234  输入数据
      */
    object Run {
    
      def main(args: Array[String]): Unit = {
    
    
        val port = 1234
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream = env.socketTextStream("localhost", port, '\n')
    
        val dataStreamMap = dataStream.map(x => x + " 增加的数据")
    
        dataStreamMap.print()
    
    
        if(args == null || args.size ==0){
          env.execute("默认作业")
        }else{
          env.execute(args(0))
        }
    
        println("结束")
    
      }
    
    }

    ```
- 输出数据
 
    ```
        1> 你好 增加的数据
        2> 发送数据 增加的数据
    ```


### flatMap
- 处理所有元素,并且把每行中的子集合，汇总成一个大集合
- 输入数据
    ```
    a b c
    e f g
    ```
- 程序
    ```
    package com.opensourceteams.module.bigdata.flink.example.stream.operator.flatmap
    
    import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
    
    /**
      * nc -lk 1234  输入数据
      */
    object Run {
    
      def main(args: Array[String]): Unit = {
    
    
        val port = 1234
        // get the execution environment
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream = env.socketTextStream("localhost", port, '\n')
    
        val dataStream2 = dataStream.flatMap(x => x.split(" "))
    
        dataStream2.print()
    
    
    
        if(args == null || args.size ==0){
          env.execute("默认作业")
        }else{
          env.execute(args(0))
        }
    
        println("结束")
    
      }
    
    
    }

    ```
- 输出数据
 
    ```
    a
    b
    c
    e
    f
    g
    ```

### filter
- 过滤数据
- 输入数据
 
    ```
    a b c
    a c
    b b
    d d
    ```

- 程序
    ```
    package com.opensourceteams.module.bigdata.flink.example.stream.operator.filter
    
    import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment}
    
    /**
      * nc -lk 1234  输入数据
      */
    object Run {
    
      def main(args: Array[String]): Unit = {
    
    
        val port = 1234
        // get the execution environment
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val dataStream = env.socketTextStream("localhost", port, '\n')
    
        val dataStreamMap = dataStream.filter( x => (x.contains("a")))
    
        dataStreamMap.print()
    
    
        if(args == null || args.size ==0){
          env.execute("默认作业")
        }else{
          env.execute(args(0))
        }
    
        println("结束")
    
      }
    
    
    
    }
    
    
    ```
- 输出数据

    ```
    3> a b c
    4> a c
    ```


### keyBy
- 指定某列为key,一般按key分组时用
- 输入数据
 
    ```
    c a b a
    ```
- 程序
 
    ```
        package com.opensourceteams.module.bigdata.flink.example.stream.operator.sum
        
        import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
        import org.apache.flink.streaming.api.windowing.time.Time
        
        /**
          * nc -lk 1234  输入数据
          */
        object Run {
        
          def main(args: Array[String]): Unit = {
        
        
            val port = 1234
            // get the execution environment
            val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
            val dataStream = env.socketTextStream("localhost", port, '\n')
        
        
            val dataStream2 = dataStream.flatMap(x => x.split(" ")).map((_,1))
              .keyBy(0)
        
              //dataStream.keyBy("someKey") // Key by field "someKey"
              //dataStream.keyBy(0) // Key by the first element of a Tuple
        
              .timeWindow(Time.seconds(2))//每2秒滚动窗口
              .sum(1)
        
        
        
        
        
            dataStream2.print()
        
        
        
            if(args == null || args.size ==0){
              env.execute("默认作业")
            }else{
              env.execute(args(0))
            }
        
            println("结束")
        
          }
        
        
        }
    
    
    ```
    
- 输出数据,数据输出顺序多线程是不固定的,但也是一样的规则取
  
- 默认并行度
    ```
        6> (a,2)
        4> (c,1)
        2> (b,1)
    ```
- 并行度为1，就先去重，取第一个元素，再按从最后一个开始,即  c a b a 变为  c a b 然后变成  c b a 

    ```
     (c,1)
     (b,1)
     (a,2)
     
    ```

### sum
- keyBy指定某列为key,一般按key分组时用,sum按key分组后求合
- 输入数据
 
    ```
    c a b a
    ```
- 程序
 
    ```
        package com.opensourceteams.module.bigdata.flink.example.stream.operator.sum
        
        import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
        import org.apache.flink.streaming.api.windowing.time.Time
        
        /**
          * nc -lk 1234  输入数据
          */
        object Run {
        
          def main(args: Array[String]): Unit = {
        
        
            val port = 1234
            // get the execution environment
            val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
            val dataStream = env.socketTextStream("localhost", port, '\n')
        
        
            val dataStream2 = dataStream.flatMap(x => x.split(" ")).map((_,1))
              .keyBy(0)
        
              //dataStream.keyBy("someKey") // Key by field "someKey"
              //dataStream.keyBy(0) // Key by the first element of a Tuple
        
              .timeWindow(Time.seconds(2))//每2秒滚动窗口
              .sum(1)
        
        
        
        
        
            dataStream2.print()
        
        
        
            if(args == null || args.size ==0){
              env.execute("默认作业")
            }else{
              env.execute(args(0))
            }
        
            println("结束")
        
          }
        
        
        }
    
    
    ```
    
- 输出数据,数据输出顺序多线程是不固定的,但也是一样的规则取
  
- 默认并行度
    ```
        6> (a,2)
        4> (c,1)
        2> (b,1)
    ```
- 并行度为1，就先去重，取第一个元素，再按从最后一个开始,即  c a b a 变为  c a b 然后变成  c b a 

    ```
     (c,1)
     (b,1)
     (a,2)
     
    ```


### reduce
- keyBy指定某列为key,一般按key分组时用,对相同的key，元素之间进行的函数运算
- 输入数据
 
    ```
   a b b c
    ```
- 程序
 
    ```
    package com.opensourceteams.module.bigdata.flink.example.stream.operator.reduce
    
    import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
    import org.apache.flink.streaming.api.windowing.time.Time
    
    /**
      * nc -lk 1234  输入数据
      */
    object Run {
    
      def main(args: Array[String]): Unit = {
    
    
        val port = 1234
        // get the execution environment
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)  //设置并行度
        val dataStream = env.socketTextStream("localhost", port, '\n')
    
    
        val dataStream2 = dataStream.flatMap(x => x.split(" ")).map((_,2))
          .keyBy(0)
    
          //dataStream.keyBy("someKey") // Key by field "someKey"
          //dataStream.keyBy(0) // Key by the first element of a Tuple
    
          .timeWindow(Time.seconds(2))//每2秒滚动窗口
            .reduce((a,b) =>  (a._1,a._2 * b._2) )
    
    
    
    
    
        dataStream2.print()
    
    
    
    
        println("=======================打印StreamPlanAsJSON=======================\n")
        println("JSON转图在线工具: https://flink.apache.org/visualizer")
        println(env.getStreamGraph.getStreamingPlanAsJSON)
        println("==================================================================\n")
    
        if(args == null || args.size ==0){
          env.execute("默认作业")
        }else{
          env.execute(args(0))
        }
    
        println("结束")
    
      }
    
    
    }
    
    ```
    
- 输出数据,数据输出顺序多线程是不固定的,但也是一样的规则取
- 默认并行度
 
```
    6> (a,2)
    4> (c,1)
    2> (b,1)


- 并行度为1，就先去重，取第一个元素，再按从最后一个开始,即  c a b a 变为  c a b 然后变成  c b a 

```

(a,2)
(c,2)
(b,4)
 
```



