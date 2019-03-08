package com.opensourceteams.module.bigdata.flink.example.datastream.worldcount.nc.eventtime

import java.util.Date

import com.alibaba.fastjson.JSON
import com.opensourceteams.module.bigdata.flink.common.ConfigurationUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object SockWordCountRunConfig {



  def main(args: Array[String]): Unit = {


    import org.apache.flink.streaming.api.scala._

    val configuration : Configuration = ConfigurationUtil.getConfiguration(true)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment;
    env.getConfig.setGlobalJobParameters(configuration)


    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)




    val dataStream = env.socketTextStream("localhost", 1234, '\n')


     // .setParallelism(3)


    dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[String] {

        val maxOutOfOrderness =  2 * 1000L // 3.5 seconds
        var currentMaxTimestamp: Long = _
        var currentTimestamp: Long = _

        override def getCurrentWatermark: Watermark =  new Watermark(currentMaxTimestamp - maxOutOfOrderness)

        override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
          val jsonObject = JSON.parseObject(element)

          val timestamp = jsonObject.getLongValue("extract_data_time")
          currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
          currentTimestamp = timestamp

        /*  println("===========watermark begin===========")
          println()
          println(new Date(currentMaxTimestamp - 20 * 1000))
          println(jsonObject)
          println("===========watermark end===========")
          println()*/
          timestamp
        }

      })
      .timeWindowAll(Time.seconds(3))

      .process(new ProcessAllWindowFunction[String,String,TimeWindow]() {
      override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {


        println()
        println("开始提交window")
        println(new Date())
        for(e <- elements) out.collect(e)
        println("结束提交window")
        println(new Date())
        println()
      }
    })

      .print()
      //.setParallelism(3)





    println("==================================以下为执行计划==================================")
    println("执行地址(firefox效果更好):https://flink.apache.org/visualizer")
    //执行计划
    println(env.getStreamGraph.getStreamingPlanAsJSON)
    println("==================================以上为执行计划 JSON串==================================\n")


    env.execute("Socket 水印作业")






    println("结束")

  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long){
    //override def toString: String = Thread.currentThread().getName + word + " : " + count
  }





}
