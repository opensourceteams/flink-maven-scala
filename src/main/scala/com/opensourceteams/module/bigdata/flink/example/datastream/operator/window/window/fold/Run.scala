package com.opensourceteams.module.bigdata.flink.example.datastream.operator.window.window.fold

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.WindowFunction
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
   // env.setParallelism(1)  //设置并行度
    val dataStream = env.socketTextStream("localhost", port, '\n')


    val dataStream2 = dataStream.flatMap(x => x.split(" ")).map((_,1))
      .keyBy(0)

      /**
        * 定义window,并指定分配元素到window的方式
        * 可以在已经分区的KeyedStream上定义Windows。 Windows根据某些特征（例如，在最后5秒内到达的数据）对每个密钥中的数据进行分组。 有关窗口的完整说明，请参见windows。
        */
      .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))


      /**
        * 按key进行处理，第一个参数，是字符串，放在每次处理的最前面第二个是表达式，第二个表达式有两个参数，第一个参数，就是第一个参数的值，第二个参数，我每次循环key时，迭代的下一个元素
        */
      .fold("字符串开始")((str, i) => { str + "-" + i} )








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
