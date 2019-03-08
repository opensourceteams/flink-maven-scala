package com.opensourceteams.module.bigdata.flink.example.datastream.operator.union

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
  * nc -lk 1234  输入数据
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

   val dataStream1 = getDataStream(env,1234,"localhost")
   val dataStream2 = getDataStream(env,12345,"localhost")

    /**
      * 只是将两个流的数据，union在一起，之后，不能再进行操作了
      */
    val dataStream3 = dataStream1.union(dataStream2)
   /* dataStream3.keyBy(0)
      .timeWindow(Time.seconds(5))//每2秒滚动窗口
      .sum(1)*/




    dataStream3.print()




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

  def getDataStream(env: StreamExecutionEnvironment,port:Int,host:String):DataStream[(String,Int)]={


    //env.setParallelism(1)  //设置并行度
    val dataStream = env.socketTextStream(host, port, '\n')

    val dataStream2 = dataStream.flatMap(x => x.split(" ")).map((_,1))
      .keyBy(0)

      .timeWindow(Time.seconds(5))//每2秒滚动窗口
      .sum(1)

    dataStream2

  }


}
