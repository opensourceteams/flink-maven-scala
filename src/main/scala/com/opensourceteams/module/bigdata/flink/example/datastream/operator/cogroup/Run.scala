package com.opensourceteams.module.bigdata.flink.example.datastream.operator.cogroup

import java.lang

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * nc -lk 1234  输入数据
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1) //由于想看数据结构，所以先设为1，这样


   val dataStream1 = getDataStream(env,1234,"localhost")
   val dataStream2 = getDataStream(env,12345,"localhost")

    val dataStream3 = dataStream1.coGroup(dataStream2)




    dataStream3.where(x => x._1).equalTo(x=> x._1)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        .apply(new CoGroupFunction[(String,Int),(String,Int),String] {
          override def coGroup(first: lang.Iterable[(String, Int)], second: lang.Iterable[(String, Int)], out: Collector[String]): Unit = {
           println("==============开始")
            println("first")
            println(first)

            println("second")
            println(second)

           /* val iteratorFirst = first.iterator()
            while (iteratorFirst.hasNext()){
              println(iteratorFirst.next())
            }

            println("second")
            val iteratorSecond = second.iterator()
            while (iteratorSecond.hasNext()){
              println(iteratorSecond.next())
            }*/

            println("==============结束")

          }
        })


    /**
      * 打印结果
      *
      * ==============开始
      * first
      * [(a,1)]
      * second
      * [(a,1), (a,1)]
      * ==============结束
      * ==============开始
      * first
      * [(c,1), (c,1)]
      * second
      * []
      * ==============结束
      * ==============开始
      * first
      * []
      * second
      * [(b,1)]
      * ==============结束
      */






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
