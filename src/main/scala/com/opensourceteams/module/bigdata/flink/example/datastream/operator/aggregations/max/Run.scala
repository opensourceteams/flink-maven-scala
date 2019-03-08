package com.opensourceteams.module.bigdata.flink.example.datastream.operator.aggregations.max

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
