# Flink1.7.2  DataStream Operator

## 源码
- https://github.com/opensourceteams/flink-maven-scala
- https://github.com/opensourceteams/flink-maven-scala/tree/master/src/main/scala/com/opensourceteams/module/bigdata/flink/example/datastream/operator

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
```

- 并行度为1，就先去重，取第一个元素，再按从最后一个开始,即  c a b a 变为  c a b 然后变成  c b a 

```

(a,2)
(c,2)
(b,4)
 
```


### fold
- 按key进行处理，第一个参数，是字符串，放在每次处理的最前面第二个是表达式，第二个表达式有两个参数，第一个参数，就是第一个参数的值，第二个参数，我每次循环key时，迭代的下一个元素
- 输入数据
```
a a b c c 
```
- 程序
```
package com.opensourceteams.module.bigdata.flink.example.stream.operator.fold

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


    val dataStream2 = dataStream.flatMap(x => x.split(" ")).map((_,1))
      .keyBy(0)


      //dataStream.keyBy("someKey") // Key by field "someKey"
      //dataStream.keyBy(0) // Key by the first element of a Tuple

      .timeWindow(Time.seconds(2))//每2秒滚动窗口
      .fold("开始字符串")((str, i) => { str + "-" + i} )





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
- 输出数据
 
```
开始字符串-(a,1)-(a,1)
开始字符串-(c,1)-(c,1)
开始字符串-(b,1)
```

## Aggregations (sum ,max,min)
### sum
- 处理所有元素,相同key进行累加
- 输入数据
```
a a c b c
```
- 程序
```
package com.opensourceteams.module.bigdata.flink.example.stream.operator.aggregations.sum

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
    //env.setParallelism(1)  //设置并行度,不设置就是默认最高并行度为的cpu ,我的四核8线程，就是最高并行度为8
    val dataStream = env.socketTextStream("localhost", port, '\n')


    val dataStream2 = dataStream.flatMap(x => x.split(" ")).map((_,1))
      .keyBy(0)

      //dataStream.keyBy("someKey") // Key by field "someKey"
      //dataStream.keyBy(0) // Key by the first element of a Tuple

      .timeWindow(Time.seconds(2))//每2秒滚动窗口
      .sum(1)





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
- 输出数据
 
```
4> (c,2)
2> (b,1)
6> (a,2)

```



### min(和minBy一样,没发现区别)
- 处理所有元素，对相同key的元素，进行求最小的值 
- 输入数据
```
b a b a a b
```
- 程序
```
package com.opensourceteams.module.bigdata.flink.example.datastream.operator.aggregations.sum


import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.scala._

/**
  * nc -lk 1234  输入数据
  */
object Run {

  def main(args: Array[String]): Unit = {


    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)  //设置并行度,不设置就是默认最高并行度为的cpu ,我的四核8线程，就是最高并行度为8
    val dataStream = env.socketTextStream("localhost", port, '\n')

    var i  = 0

    val dataStream2 = dataStream.flatMap(x => x.split(" ")).map( x => {
      i = i + 1
      (x,i)
    })
      .keyBy(0)

      .timeWindow(Time.seconds(2))//每2秒滚动窗口
      .min(1)





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
- 输出数据

- 最终输出数据，顺序取决于线程的调用
 
```
2> (b,1)
6> (a,2)
```

- 中间输出数据
```aidl
6> (a,2)
2> (b,1)
6> (a,4)
2> (b,3)
6> (a,5)
2> (b,6)
```



### max(和maxBy一样)
- 处理所有元素，对相同key的元素，进行求最大的值 
- 输入数据

```
b a b a a b
```
- 程序

```
package com.opensourceteams.module.bigdata.flink.example.datastream.operator.aggregations.sum


import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.scala._

/**
  * nc -lk 1234  输入数据
  */
object Run {

  def main(args: Array[String]): Unit = {


    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)  //设置并行度,不设置就是默认最高并行度为的cpu ,我的四核8线程，就是最高并行度为8
    val dataStream = env.socketTextStream("localhost", port, '\n')

    var i  = 0

    val dataStream2 = dataStream.flatMap(x => x.split(" ")).map( x => {
      i = i + 1
      (x,i)
    })
      .keyBy(0)

      .timeWindow(Time.seconds(2))//每2秒滚动窗口
      .max(1)





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

- 输出数据

- 最终输出数据，顺序取决于线程的调用
 
```

2> (b,6)
6> (a,5)

```

- 中间输出数据

```

6> (a,2)
2> (b,1)
6> (a,4)
2> (b,3)
6> (a,5)
2> (b,6)

```



## Window
### window
- 定义window,并指定分配元素到window的方式
- 可以在已经分区的KeyedStream上定义Windows。 Windows根据某些特征（例如，在最后5秒内到达的数据）对每个密钥中的数据进行分组。 有关窗口的完整说明，请参见windows。
- 输入数据

```
b a b a a b

```

- 程序

```
package com.opensourceteams.module.bigdata.flink.example.datastream.operator.window.window

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{ TumblingProcessingTimeWindows}
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


    val dataStream2 = dataStream.flatMap(x => x.split(" ")).map((_,1))
      .keyBy(0)

      /**
        * 定义window,并指定分配元素到window的方式
        * 可以在已经分区的KeyedStream上定义Windows。 Windows根据某些特征（例如，在最后5秒内到达的数据）对每个密钥中的数据进行分组。 有关窗口的完整说明，请参见windows。
        */
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))

      .sum(1)





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

- 输出数据
 
(b,3)
(a,3)
```



### WindowAll
- 配合process.ProcessAllWindowFunction函数，该函数的参数elements: Iterable[(String, Int)] 即为当前window的所有元素，可以进行处理，再发给下游sink
- 输入数据

```
b a b a a b

```
- 程序

```
package com.opensourceteams.module.bigdata.flink.example.datastream.operator.window.windowAll

import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * nc -lk 1234  输入数据
  */
object Run {

  def main(args: Array[String]): Unit = {


    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)  //设置并行度
    val dataStream = env.socketTextStream("localhost", port, '\n')


    dataStream.flatMap(x => x.split(" ")).map((_,1))
      .keyBy(0)


      .windowAll( TumblingProcessingTimeWindows.of(Time.seconds(2)))
        .process(new ProcessAllWindowFunction[(String, Int),(String, Int),TimeWindow] {
          override def process(context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
            //可以对当前window中的所有元素进行操作，处理后，再发送给Sink
            for(element <- elements) out.collect(element)
          }
        })

        .print()





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

- 输出数据
 
 
```

8> (a,1)
7> (b,1)
3> (a,1)
2> (a,1)
1> (b,1)
4> (b,1)

```