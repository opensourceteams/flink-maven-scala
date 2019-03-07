package com.opensourceteams.module.bigdata.flink.example.stream.connectors.kafka.eventtime

import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import com.opensourceteams.module.bigdata.flink.common.ConfigurationUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector


object ReadKafkaDataRun2 {



  def main(args: Array[String]): Unit = {


    // get the execution environment
   // val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val configuration : Configuration = ConfigurationUtil.getConfiguration(true)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1,configuration)


    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")
    import org.apache.flink.streaming.api.scala._
    env.addSource(new FlinkKafkaConsumer[String]("topic1", new SimpleStringSchema(), properties))

     // .setParallelism(3)


      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[String] {

        val maxOutOfOrderness =  1 * 1000L // 3.5 seconds
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


      .print()
      //.setParallelism(3)





    println("==================================以下为执行计划==================================")
    println("执行地址(firefox效果更好):https://flink.apache.org/visualizer")
    //执行计划
    println(env.getStreamGraph.getStreamingPlanAsJSON)
    println("==================================以上为执行计划 JSON串==================================\n")


    env.execute("读取kafka数据")






    println("结束")

  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long){
    //override def toString: String = Thread.currentThread().getName + word + " : " + count
  }


  def getConfiguration(isDebug:Boolean = false):Configuration = {

    val configuration : Configuration = new Configuration()

    if(isDebug){
      val timeout = "100000 s"
      val timeoutHeartbeatPause = "1000000 s"
      configuration.setString("akka.ask.timeout",timeout)
      configuration.setString("akka.lookup.timeout",timeout)
      configuration.setString("akka.tcp.timeout",timeout)
      configuration.setString("akka.transport.heartbeat.interval",timeout)
      configuration.setString("akka.transport.heartbeat.pause",timeoutHeartbeatPause)
      configuration.setString("akka.watch.heartbeat.pause",timeout)
      configuration.setInteger("heartbeat.interval",10000000)
      configuration.setInteger("heartbeat.timeout",50000000)
    }


    configuration
  }


}
