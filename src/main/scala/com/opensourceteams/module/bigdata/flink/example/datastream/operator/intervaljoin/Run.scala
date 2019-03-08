package com.opensourceteams.module.bigdata.flink.example.datastream.operator.intervaljoin

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * nc -lk 1234  输入数据
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)


   val dataStream1 = getDataStream(env,1234,"localhost")
   val dataStream2 = getDataStream(env,12345,"localhost")

    val dataStream3 = dataStream1.keyBy(0).intervalJoin(dataStream2.keyBy(0))




    dataStream3.between(Time.seconds(-5), Time.seconds(5))
      //.upperBoundExclusive(true) // optional
      //.lowerBoundExclusive(true) // optional
        .process(new ProcessJoinFunction[(String,Int),(String,Int),String] {
      override def processElement(left: (String, Int), right: (String, Int), ctx: ProcessJoinFunction[(String, Int), (String, Int), String]#Context, out: Collector[String]): Unit = {
        println(left + "," + right)
        out.collect( left + "," + right)
      }
    })





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

    dataStream2

  }


}
