# Flink 1.7.2 业务时间戳分析流式数据源码分析

## 源码
- https://github.com/opensourceteams/flink-maven-scala

## 概述
- 由于Flink默认的ProcessTime是按Window收到Source发射过来的数据的时间，来算了，也就是按Flink程序接收的时间来进行计算，但实际业务，处理周期性的数据时，每5分钟内的数据，每1个小时内的数据进行分析，实际是业务源发生的时间来做为实际时间，所以用Flink的EventTime和Watermark来处理这个问题
- 指定Env为EventTime
- 调置数据流assignTimestampsAndWatermarks函数，由AssignerWithPeriodicWatermarks中的extractTimestamp()函数提取实际业务时间，getCurrentWatermark得到最新的时间，这个会对每个元素算一次，拿最大的当做计算时间，如果当前时间，大于上一次的时间间隔 + 这里设置的延时时间，就会结束上一个Window,也就是对这一段时间的Window进行操作
- 本程序以指定业务时间，来做为统计时间


## 程序
```
package com.opensourceteams.module.bigdata.flink.example.stream.worldcount.nc.eventtime

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


object SockWordCountRun {



  def main(args: Array[String]): Unit = {


    // get the execution environment
   // val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val configuration : Configuration = ConfigurationUtil.getConfiguration(true)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1,configuration)


    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)



    import org.apache.flink.streaming.api.scala._
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

```



## 输入数据
- 下一次输入数据，只要比上一次输入数据，大5秒，就可以输出上一window数据，时间以data_time 字段为准，转成字符串格式为extract_data_time
- nc -lk 123  来输入数据 
```
{"no":0,"extract_data_time":"2019-03-06 11:40:38","data_time":1551843638000,"message":"0  2019-03-06 11:40:38","send_data_string":"2019-03-06 21:03:43"}
{"no":1,"extract_data_time":"2019-03-06 11:40:39","data_time":1551843639000,"message":"1  2019-03-06 11:40:39","send_data_string":"2019-03-06 21:03:43"}
{"no":2,"extract_data_time":"2019-03-06 11:40:40","data_time":1551843640000,"message":"2  2019-03-06 11:40:40","send_data_string":"2019-03-06 21:03:43"}
{"no":3,"extract_data_time":"2019-03-06 11:40:41","data_time":1551843641000,"message":"3  2019-03-06 11:40:41","send_data_string":"2019-03-06 21:03:43"}
{"no":4,"extract_data_time":"2019-03-06 11:40:42","data_time":1551843642000,"message":"4  2019-03-06 11:40:42","send_data_string":"2019-03-06 21:03:43"}
{"no":5,"extract_data_time":"2019-03-06 11:40:43","data_time":1551843643000,"message":"5  2019-03-06 11:40:43","send_data_string":"2019-03-06 21:03:43"}
{"no":6,"extract_data_time":"2019-03-06 11:40:44","data_time":1551843644000,"message":"6  2019-03-06 11:40:44","send_data_string":"2019-03-06 21:03:43"}
{"no":7,"extract_data_time":"2019-03-06 11:40:45","data_time":1551843645000,"message":"7  2019-03-06 11:40:45","send_data_string":"2019-03-06 21:03:43"}
{"no":8,"extract_data_time":"2019-03-06 11:40:46","data_time":1551843646000,"message":"8  2019-03-06 11:40:46","send_data_string":"2019-03-06 21:03:43"}
{"no":9,"extract_data_time":"2019-03-06 11:40:47","data_time":1551843647000,"message":"9  2019-03-06 11:40:47","send_data_string":"2019-03-06 21:03:43"}
{"no":10,"extract_data_time":"2019-03-06 11:40:48","data_time":1551843648000,"message":"10  2019-03-06 11:40:48","send_data_string":"2019-03-06 21:03:43"}
{"no":11,"extract_data_time":"2019-03-06 11:40:49","data_time":1551843649000,"message":"11  2019-03-06 11:40:49","send_data_string":"2019-03-06 21:03:43"}
{"no":12,"extract_data_time":"2019-03-06 11:40:50","data_time":1551843650000,"message":"12  2019-03-06 11:40:50","send_data_string":"2019-03-06 21:03:43"}
{"no":13,"extract_data_time":"2019-03-06 11:40:51","data_time":1551843651000,"message":"13  2019-03-06 11:40:51","send_data_string":"2019-03-06 21:03:43"}
{"no":14,"extract_data_time":"2019-03-06 11:40:52","data_time":1551843652000,"message":"14  2019-03-06 11:40:52","send_data_string":"2019-03-06 21:03:43"}
{"no":15,"extract_data_time":"2019-03-06 11:40:53","data_time":1551843653000,"message":"15  2019-03-06 11:40:53","send_data_string":"2019-03-06 21:03:43"}
{"no":16,"extract_data_time":"2019-03-06 11:40:54","data_time":1551843654000,"message":"16  2019-03-06 11:40:54","send_data_string":"2019-03-06 21:03:43"}
{"no":17,"extract_data_time":"2019-03-06 11:40:55","data_time":1551843655000,"message":"17  2019-03-06 11:40:55","send_data_string":"2019-03-06 21:03:43"}
{"no":18,"extract_data_time":"2019-03-06 11:40:56","data_time":1551843656000,"message":"18  2019-03-06 11:40:56","send_data_string":"2019-03-06 21:03:43"}
{"no":19,"extract_data_time":"2019-03-06 11:40:57","data_time":1551843657000,"message":"19  2019-03-06 11:40:57","send_data_string":"2019-03-06 21:03:43"}
{"no":20,"extract_data_time":"2019-03-06 11:40:58","data_time":1551843658000,"message":"20  2019-03-06 11:40:58","send_data_string":"2019-03-06 21:03:43"}
{"no":21,"extract_data_time":"2019-03-06 11:40:59","data_time":1551843659000,"message":"21  2019-03-06 11:40:59","send_data_string":"2019-03-06 21:03:43"}
{"no":22,"extract_data_time":"2019-03-06 11:41:00","data_time":1551843660000,"message":"22  2019-03-06 11:41:00","send_data_string":"2019-03-06 21:03:43"}
{"no":23,"extract_data_time":"2019-03-06 11:41:01","data_time":1551843661000,"message":"23  2019-03-06 11:41:01","send_data_string":"2019-03-06 21:03:43"}
{"no":24,"extract_data_time":"2019-03-06 11:41:02","data_time":1551843662000,"message":"24  2019-03-06 11:41:02","send_data_string":"2019-03-06 21:03:43"}
{"no":25,"extract_data_time":"2019-03-06 11:41:03","data_time":1551843663000,"message":"25  2019-03-06 11:41:03","send_data_string":"2019-03-06 21:03:43"}
{"no":26,"extract_data_time":"2019-03-06 11:41:04","data_time":1551843664000,"message":"26  2019-03-06 11:41:04","send_data_string":"2019-03-06 21:03:43"}
{"no":27,"extract_data_time":"2019-03-06 11:41:05","data_time":1551843665000,"message":"27  2019-03-06 11:41:05","send_data_string":"2019-03-06 21:03:43"}
{"no":28,"extract_data_time":"2019-03-06 11:41:06","data_time":1551843666000,"message":"28  2019-03-06 11:41:06","send_data_string":"2019-03-06 21:03:43"}
{"no":29,"extract_data_time":"2019-03-06 11:41:07","data_time":1551843667000,"message":"29  2019-03-06 11:41:07","send_data_string":"2019-03-06 21:03:43"}
{"no":30,"extract_data_time":"2019-03-06 11:41:08","data_time":1551843668000,"message":"30  2019-03-06 11:41:08","send_data_string":"2019-03-06 21:03:43"}
{"no":31,"extract_data_time":"2019-03-06 11:41:09","data_time":1551843669000,"message":"31  2019-03-06 11:41:09","send_data_string":"2019-03-06 21:03:43"}
{"no":32,"extract_data_time":"2019-03-06 11:41:10","data_time":1551843670000,"message":"32  2019-03-06 11:41:10","send_data_string":"2019-03-06 21:03:43"}
{"no":33,"extract_data_time":"2019-03-06 11:41:11","data_time":1551843671000,"message":"33  2019-03-06 11:41:11","send_data_string":"2019-03-06 21:03:43"}
{"no":34,"extract_data_time":"2019-03-06 11:41:12","data_time":1551843672000,"message":"34  2019-03-06 11:41:12","send_data_string":"2019-03-06 21:03:43"}
{"no":35,"extract_data_time":"2019-03-06 11:41:13","data_time":1551843673000,"message":"35  2019-03-06 11:41:13","send_data_string":"2019-03-06 21:03:43"}
{"no":36,"extract_data_time":"2019-03-06 11:41:14","data_time":1551843674000,"message":"36  2019-03-06 11:41:14","send_data_string":"2019-03-06 21:03:43"}
{"no":37,"extract_data_time":"2019-03-06 11:41:15","data_time":1551843675000,"message":"37  2019-03-06 11:41:15","send_data_string":"2019-03-06 21:03:43"}
{"no":38,"extract_data_time":"2019-03-06 11:41:16","data_time":1551843676000,"message":"38  2019-03-06 11:41:16","send_data_string":"2019-03-06 21:03:43"}
{"no":39,"extract_data_time":"2019-03-06 11:41:17","data_time":1551843677000,"message":"39  2019-03-06 11:41:17","send_data_string":"2019-03-06 21:03:43"}
{"no":40,"extract_data_time":"2019-03-06 11:41:18","data_time":1551843678000,"message":"40  2019-03-06 11:41:18","send_data_string":"2019-03-06 21:03:43"}
{"no":41,"extract_data_time":"2019-03-06 11:41:19","data_time":1551843679000,"message":"41  2019-03-06 11:41:19","send_data_string":"2019-03-06 21:03:43"}
{"no":42,"extract_data_time":"2019-03-06 11:41:20","data_time":1551843680000,"message":"42  2019-03-06 11:41:20","send_data_string":"2019-03-06 21:03:43"}
{"no":43,"extract_data_time":"2019-03-06 11:41:21","data_time":1551843681000,"message":"43  2019-03-06 11:41:21","send_data_string":"2019-03-06 21:03:43"}
{"no":44,"extract_data_time":"2019-03-06 11:41:22","data_time":1551843682000,"message":"44  2019-03-06 11:41:22","send_data_string":"2019-03-06 21:03:43"}
{"no":45,"extract_data_time":"2019-03-06 11:41:23","data_time":1551843683000,"message":"45  2019-03-06 11:41:23","send_data_string":"2019-03-06 21:03:43"}
{"no":46,"extract_data_time":"2019-03-06 11:41:24","data_time":1551843684000,"message":"46  2019-03-06 11:41:24","send_data_string":"2019-03-06 21:03:43"}
{"no":47,"extract_data_time":"2019-03-06 11:41:25","data_time":1551843685000,"message":"47  2019-03-06 11:41:25","send_data_string":"2019-03-06 21:03:43"}
{"no":48,"extract_data_time":"2019-03-06 11:41:26","data_time":1551843686000,"message":"48  2019-03-06 11:41:26","send_data_string":"2019-03-06 21:03:43"}
{"no":49,"extract_data_time":"2019-03-06 11:41:27","data_time":1551843687000,"message":"49  2019-03-06 11:41:27","send_data_string":"2019-03-06 21:03:43"}
{"no":50,"extract_data_time":"2019-03-06 11:41:28","data_time":1551843688000,"message":"50  2019-03-06 11:41:28","send_data_string":"2019-03-06 21:03:43"}
{"no":51,"extract_data_time":"2019-03-06 11:41:29","data_time":1551843689000,"message":"51  2019-03-06 11:41:29","send_data_string":"2019-03-06 21:03:43"}
{"no":52,"extract_data_time":"2019-03-06 11:41:30","data_time":1551843690000,"message":"52  2019-03-06 11:41:30","send_data_string":"2019-03-06 21:03:43"}
{"no":53,"extract_data_time":"2019-03-06 11:41:31","data_time":1551843691000,"message":"53  2019-03-06 11:41:31","send_data_string":"2019-03-06 21:03:43"}
{"no":54,"extract_data_time":"2019-03-06 11:41:32","data_time":1551843692000,"message":"54  2019-03-06 11:41:32","send_data_string":"2019-03-06 21:03:43"}
{"no":55,"extract_data_time":"2019-03-06 11:41:33","data_time":1551843693000,"message":"55  2019-03-06 11:41:33","send_data_string":"2019-03-06 21:03:43"}
{"no":56,"extract_data_time":"2019-03-06 11:41:34","data_time":1551843694000,"message":"56  2019-03-06 11:41:34","send_data_string":"2019-03-06 21:03:43"}
{"no":57,"extract_data_time":"2019-03-06 11:41:35","data_time":1551843695000,"message":"57  2019-03-06 11:41:35","send_data_string":"2019-03-06 21:03:43"}
{"no":58,"extract_data_time":"2019-03-06 11:41:36","data_time":1551843696000,"message":"58  2019-03-06 11:41:36","send_data_string":"2019-03-06 21:03:43"}
{"no":59,"extract_data_time":"2019-03-06 11:41:37","data_time":1551843697000,"message":"59  2019-03-06 11:41:37","send_data_string":"2019-03-06 21:03:43"}
{"no":60,"extract_data_time":"2019-03-06 11:41:38","data_time":1551843698000,"message":"60  2019-03-06 11:41:38","send_data_string":"2019-03-06 21:03:43"}
{"no":61,"extract_data_time":"2019-03-06 11:41:39","data_time":1551843699000,"message":"61  2019-03-06 11:41:39","send_data_string":"2019-03-06 21:03:43"}
{"no":62,"extract_data_time":"2019-03-06 11:41:40","data_time":1551843700000,"message":"62  2019-03-06 11:41:40","send_data_string":"2019-03-06 21:03:43"}
{"no":63,"extract_data_time":"2019-03-06 11:41:41","data_time":1551843701000,"message":"63  2019-03-06 11:41:41","send_data_string":"2019-03-06 21:03:43"}
{"no":64,"extract_data_time":"2019-03-06 11:41:42","data_time":1551843702000,"message":"64  2019-03-06 11:41:42","send_data_string":"2019-03-06 21:03:43"}
{"no":65,"extract_data_time":"2019-03-06 11:41:43","data_time":1551843703000,"message":"65  2019-03-06 11:41:43","send_data_string":"2019-03-06 21:03:43"}
{"no":66,"extract_data_time":"2019-03-06 11:41:44","data_time":1551843704000,"message":"66  2019-03-06 11:41:44","send_data_string":"2019-03-06 21:03:43"}
{"no":67,"extract_data_time":"2019-03-06 11:41:45","data_time":1551843705000,"message":"67  2019-03-06 11:41:45","send_data_string":"2019-03-06 21:03:43"}
{"no":68,"extract_data_time":"2019-03-06 11:41:46","data_time":1551843706000,"message":"68  2019-03-06 11:41:46","send_data_string":"2019-03-06 21:03:43"}
{"no":69,"extract_data_time":"2019-03-06 11:41:47","data_time":1551843707000,"message":"69  2019-03-06 11:41:47","send_data_string":"2019-03-06 21:03:43"}
{"no":70,"extract_data_time":"2019-03-06 11:41:48","data_time":1551843708000,"message":"70  2019-03-06 11:41:48","send_data_string":"2019-03-06 21:03:43"}
{"no":71,"extract_data_time":"2019-03-06 11:41:49","data_time":1551843709000,"message":"71  2019-03-06 11:41:49","send_data_string":"2019-03-06 21:03:43"}
{"no":72,"extract_data_time":"2019-03-06 11:41:50","data_time":1551843710000,"message":"72  2019-03-06 11:41:50","send_data_string":"2019-03-06 21:03:43"}
{"no":73,"extract_data_time":"2019-03-06 11:41:51","data_time":1551843711000,"message":"73  2019-03-06 11:41:51","send_data_string":"2019-03-06 21:03:43"}
{"no":74,"extract_data_time":"2019-03-06 11:41:52","data_time":1551843712000,"message":"74  2019-03-06 11:41:52","send_data_string":"2019-03-06 21:03:43"}
{"no":75,"extract_data_time":"2019-03-06 11:41:53","data_time":1551843713000,"message":"75  2019-03-06 11:41:53","send_data_string":"2019-03-06 21:03:43"}
{"no":76,"extract_data_time":"2019-03-06 11:41:54","data_time":1551843714000,"message":"76  2019-03-06 11:41:54","send_data_string":"2019-03-06 21:03:43"}
{"no":77,"extract_data_time":"2019-03-06 11:41:55","data_time":1551843715000,"message":"77  2019-03-06 11:41:55","send_data_string":"2019-03-06 21:03:43"}
{"no":78,"extract_data_time":"2019-03-06 11:41:56","data_time":1551843716000,"message":"78  2019-03-06 11:41:56","send_data_string":"2019-03-06 21:03:43"}
{"no":79,"extract_data_time":"2019-03-06 11:41:57","data_time":1551843717000,"message":"79  2019-03-06 11:41:57","send_data_string":"2019-03-06 21:03:43"}
{"no":80,"extract_data_time":"2019-03-06 11:41:58","data_time":1551843718000,"message":"80  2019-03-06 11:41:58","send_data_string":"2019-03-06 21:03:43"}
{"no":81,"extract_data_time":"2019-03-06 11:41:59","data_time":1551843719000,"message":"81  2019-03-06 11:41:59","send_data_string":"2019-03-06 21:03:43"}
{"no":82,"extract_data_time":"2019-03-06 11:42:00","data_time":1551843720000,"message":"82  2019-03-06 11:42:00","send_data_string":"2019-03-06 21:03:43"}
{"no":83,"extract_data_time":"2019-03-06 11:42:01","data_time":1551843721000,"message":"83  2019-03-06 11:42:01","send_data_string":"2019-03-06 21:03:43"}
{"no":84,"extract_data_time":"2019-03-06 11:42:02","data_time":1551843722000,"message":"84  2019-03-06 11:42:02","send_data_string":"2019-03-06 21:03:43"}
{"no":85,"extract_data_time":"2019-03-06 11:42:03","data_time":1551843723000,"message":"85  2019-03-06 11:42:03","send_data_string":"2019-03-06 21:03:43"}
{"no":86,"extract_data_time":"2019-03-06 11:42:04","data_time":1551843724000,"message":"86  2019-03-06 11:42:04","send_data_string":"2019-03-06 21:03:43"}
{"no":87,"extract_data_time":"2019-03-06 11:42:05","data_time":1551843725000,"message":"87  2019-03-06 11:42:05","send_data_string":"2019-03-06 21:03:43"}
{"no":88,"extract_data_time":"2019-03-06 11:42:06","data_time":1551843726000,"message":"88  2019-03-06 11:42:06","send_data_string":"2019-03-06 21:03:43"}
{"no":89,"extract_data_time":"2019-03-06 11:42:07","data_time":1551843727000,"message":"89  2019-03-06 11:42:07","send_data_string":"2019-03-06 21:03:43"}
{"no":90,"extract_data_time":"2019-03-06 11:42:08","data_time":1551843728000,"message":"90  2019-03-06 11:42:08","send_data_string":"2019-03-06 21:03:43"}
{"no":91,"extract_data_time":"2019-03-06 11:42:09","data_time":1551843729000,"message":"91  2019-03-06 11:42:09","send_data_string":"2019-03-06 21:03:43"}
{"no":92,"extract_data_time":"2019-03-06 11:42:10","data_time":1551843730000,"message":"92  2019-03-06 11:42:10","send_data_string":"2019-03-06 21:03:43"}
{"no":93,"extract_data_time":"2019-03-06 11:42:11","data_time":1551843731000,"message":"93  2019-03-06 11:42:11","send_data_string":"2019-03-06 21:03:43"}
{"no":94,"extract_data_time":"2019-03-06 11:42:12","data_time":1551843732000,"message":"94  2019-03-06 11:42:12","send_data_string":"2019-03-06 21:03:43"}
{"no":95,"extract_data_time":"2019-03-06 11:42:13","data_time":1551843733000,"message":"95  2019-03-06 11:42:13","send_data_string":"2019-03-06 21:03:43"}
{"no":96,"extract_data_time":"2019-03-06 11:42:14","data_time":1551843734000,"message":"96  2019-03-06 11:42:14","send_data_string":"2019-03-06 21:03:43"}
{"no":97,"extract_data_time":"2019-03-06 11:42:15","data_time":1551843735000,"message":"97  2019-03-06 11:42:15","send_data_string":"2019-03-06 21:03:43"}
{"no":98,"extract_data_time":"2019-03-06 11:42:16","data_time":1551843736000,"message":"98  2019-03-06 11:42:16","send_data_string":"2019-03-06 21:03:43"}
{"no":99,"extract_data_time":"2019-03-06 11:42:17","data_time":1551843737000,"message":"99  2019-03-06 11:42:17","send_data_string":"2019-03-06 21:03:43"}
{"no":100,"extract_data_time":"2019-03-06 11:42:18","data_time":1551843738000,"message":"100  2019-03-06 11:42:18","send_data_string":"2019-03-06 21:03:43"}

```


## ExecutionGraph.scheduleEager

-  第一部分: 主是要source,分为两小步，第一步，通过socket读取数据，第二步，通过时间戳/水印处理source
-  (Source: Socket Stream -> Timestamps/Watermarks (1/1))
-  第二部分，分为两部分，第一步部TriggerWindow，调用ProcessAllWindowFunction函数，处理当前window元素，再到第二部Sink
-  (TriggerWindow(TumblingEventTimeWindows(100000000000), ListStateDescriptor{name=window-contents, defaultValue=null, serializer=org.apache.flink.api.common.typeutils.base.ListSerializer@1e4a7dd4}, EventTimeTrigger(), AllWindowedStream.process(AllWindowedStream.scala:593)) -> Sink: Print to Std. Out (1/1))
- 详情
 
```
0 = {Execution@5366} "Attempt #0 (Source: Socket Stream -> Timestamps/Watermarks (1/1)) @ org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot@5987f5d2 - [SCHEDULED]"
1 = {Execution@5371} "Attempt #0 (TriggerWindow(TumblingEventTimeWindows(100000000000), ListStateDescriptor{name=window-contents, defaultValue=null, serializer=org.apache.flink.api.common.typeutils.base.ListSerializer@1e4a7dd4}, EventTimeTrigger(), AllWindowedStream.process(AllWindowedStream.scala:593)) -> Sink: Print to Std. Out (1/1)) @ org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot@4cb70b36 - [SCHEDULED]"

```


## Source (Task)
- operatorChain.allOperators
- Source 任务，分两个operator,第一个是StreamSource,读取Socket中的数据，第二个是TimestampsAndPeriodicWatermarksOperator,处理Source中的时间戳问题

```
0 = {TimestampsAndPeriodicWatermarksOperator@7820} 
1 = {StreamSource@7785} 
```
---
### Source(operator StreamSource)
---

### SourceStreamTask.run
- 调用StreamSource.run()函数

```
	protected void run() throws Exception {
		headOperator.run(getCheckpointLock(), getStreamStatusMaintainer());
	}
```
### 调用StreamSource.run()
```
	public void run(final Object lockingObject, final StreamStatusMaintainer streamStatusMaintainer) throws Exception {
		run(lockingObject, streamStatusMaintainer, output);
	}
```

### 调用StreamSource.run()
- 调用userFunction.run()函数，调用为SocketTextStreamFunction.run()函数
```
public void run(final Object lockingObject,
			final StreamStatusMaintainer streamStatusMaintainer,
			final Output<StreamRecord<OUT>> collector) throws Exception {

		final TimeCharacteristic timeCharacteristic = getOperatorConfig().getTimeCharacteristic();

		final Configuration configuration = this.getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();
		final long latencyTrackingInterval = getExecutionConfig().isLatencyTrackingConfigured()
			? getExecutionConfig().getLatencyTrackingInterval()
			: configuration.getLong(MetricOptions.LATENCY_INTERVAL);

		LatencyMarksEmitter<OUT> latencyEmitter = null;
		if (latencyTrackingInterval > 0) {
			latencyEmitter = new LatencyMarksEmitter<>(
				getProcessingTimeService(),
				collector,
				latencyTrackingInterval,
				this.getOperatorID(),
				getRuntimeContext().getIndexOfThisSubtask());
		}

		final long watermarkInterval = getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();

		this.ctx = StreamSourceContexts.getSourceContext(
			timeCharacteristic,
			getProcessingTimeService(),
			lockingObject,
			streamStatusMaintainer,
			collector,
			watermarkInterval,
			-1);

		try {
			userFunction.run(ctx);

			// if we get here, then the user function either exited after being done (finite source)
			// or the function was canceled or stopped. For the finite source case, we should emit
			// a final watermark that indicates that we reached the end of event-time
			if (!isCanceledOrStopped()) {
				ctx.emitWatermark(Watermark.MAX_WATERMARK);
			}
		} finally {
			// make sure that the context is closed in any case
			ctx.close();
			if (latencyEmitter != null) {
				latencyEmitter.close();
			}
		}
	}

```


### SocketTextStreamFunction.run
- 一次读取8kb数据，读取Socket中的数据放到缓存中,再按行处理缓存中的数据
- ctx.collect(record),把从sorcket得以的一行数据作为参数，调用StreamSourceContexts.WatermarkContext.collect

```
public void run(SourceContext<String> ctx) throws Exception {
		final StringBuilder buffer = new StringBuilder();
		long attempt = 0;

		while (isRunning) {

			try (Socket socket = new Socket()) {
				currentSocket = socket;

				LOG.info("Connecting to server socket " + hostname + ':' + port);
				socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

					char[] cbuf = new char[8192];
					int bytesRead;
					while (isRunning && (bytesRead = reader.read(cbuf)) != -1) {
						buffer.append(cbuf, 0, bytesRead);
						int delimPos;
						while (buffer.length() >= delimiter.length() && (delimPos = buffer.indexOf(delimiter)) != -1) {
							String record = buffer.substring(0, delimPos);
							// truncate trailing carriage return
							if (delimiter.equals("\n") && record.endsWith("\r")) {
								record = record.substring(0, record.length() - 1);
							}
							ctx.collect(record);
							buffer.delete(0, delimPos + delimiter.length());
						}
					}
				}
			}

			// if we dropped out of this loop due to an EOF, sleep and retry
			if (isRunning) {
				attempt++;
				if (maxNumRetries == -1 || attempt < maxNumRetries) {
					LOG.warn("Lost connection to server socket. Retrying in " + delayBetweenRetries + " msecs...");
					Thread.sleep(delayBetweenRetries);
				}
				else {
					// this should probably be here, but some examples expect simple exists of the stream source
					// throw new EOFException("Reached end of stream and reconnects are not enabled.");
					break;
				}
			}
		}

		// collect trailing data
		if (buffer.length() > 0) {
			ctx.collect(buffer.toString());
		}
	}
```

### StreamSourceContexts.WatermarkContext
- 调用StreamSourceContexts.ManualWatermarkContext.processAndCollect()函数

```
	public void collect(T element) {
			synchronized (checkpointLock) {
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);

				if (nextCheck != null) {
					this.failOnNextCheck = false;
				} else {
					scheduleNextIdleDetectionTask();
				}

				processAndCollect(element);
			}
		}
```
### StreamSourceContexts.ManualWatermarkContext.processAndCollect()
- 调用AbstractStreamOperator.CountingOutput.collect
```
	protected void processAndCollect(T element) {
			output.collect(reuse.replace(element));
		}
```

### AbstractStreamOperator.CountingOutput.collect
- 调用OperatorChain.CopyingChainingOutput.collect

```
public void collect(StreamRecord<OUT> record) {
			numRecordsOut.inc();
			output.collect(record);
		}
```

### OperatorChain.CopyingChainingOutput.collect
- 调用OperatorChain.CopyingChainingOutput.pushToOperator()
```
public void collect(StreamRecord<T> record) {
			if (this.outputTag != null) {
				// we are only responsible for emitting to the main input
				return;
			}

			pushToOperator(record);
		}
```
### Source(operator TimestampsAndPeriodicWatermarksOperator)

### OperatorChain.CopyingChainingOutput.pushToOperator()
- 这里调用source中的第一个二个operator  TimestampsAndPeriodicWatermarksOperator.processElement() 处理元素

```
protected <X> void pushToOperator(StreamRecord<X> record) {
			try {
				// we know that the given outputTag matches our OutputTag so the record
				// must be of the type that our operator (and Serializer) expects.
				@SuppressWarnings("unchecked")
				StreamRecord<T> castRecord = (StreamRecord<T>) record;

				numRecordsIn.inc();
				StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
				operator.setKeyContextElement1(copy);
				operator.processElement(copy);
			} catch (ClassCastException e) {
				if (outputTag != null) {
					// Enrich error message
					ClassCastException replace = new ClassCastException(
						String.format(
							"%s. Failed to push OutputTag with id '%s' to operator. " +
								"This can occur when multiple OutputTags with different types " +
								"but identical names are being used.",
							e.getMessage(),
							outputTag.getId()));

					throw new ExceptionInChainedOperatorException(replace);
				} else {
					throw new ExceptionInChainedOperatorException(e);
				}
			} catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}

		}
	}
```


###  TimestampsAndPeriodicWatermarksOperator.processElement()
- userFunction指的是 dataStream.assignTimestampsAndWatermarks(指定的函数),调的为AssignerWithPeriodicWatermarks(),注意这里边两个函数
extractTimestamp()提取时间戳，getCurrentWatermark()得到当前的水印
- element.replace(element.getValue(), newTimestamp)给当前元素调置时间戳，时间戳为AssignerWithPeriodicWatermarks.extractTimestamp()得到的值
- 调用AbstractStreamOperator.CountingOutput.collect()

```
	public void processElement(StreamRecord<T> element) throws Exception {
		final long newTimestamp = userFunction.extractTimestamp(element.getValue(),
				element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);

		output.collect(element.replace(element.getValue(), newTimestamp));
	}
```

### AbstractStreamOperator.CountingOutput.collect()
- 调用RecordWriterOutput.collect
- 调用RecordWriter.emit()进行按key的hash进行分区(如果没有进行map等操作，就是按行号进行hash分区)，并且发送给下游

```
public void collect(StreamRecord<OUT> record) {
			numRecordsOut.inc();
			output.collect(record);
		}
```








---
## Window(Task)
- operatorChain.allOperators

```
0 = {StreamSink@6060} 
1 = {WindowOperator@6041} 
```
### Window(WindowOperator)
### OneInputStreamTask.run()
- 处理Window元素,加时间戳/水印，分配window,加Trigger
- 调用StreamInputProcessor.processInput()处理

```
	protected void run() throws Exception {
		// cache processor reference on the stack, to make the code more JIT friendly
		final StreamInputProcessor<IN> inputProcessor = this.inputProcessor;

		while (running && inputProcessor.processInput()) {
			// all the work happens in the "processInput" method
		}
	}
```

### StreamInputProcessor.processInput()
- 调用barrierHandler.getNextNonBlocked(),调用BarrierTracker.getNextNonBlocked()处理Source发过来的数据
- StreamElement recordOrMark = deserializationDelegate.getInstance(); //得到source 发射过来的一条数据，进行反序列化
- 设置key streamOperator.setKeyContextElement1(record);
- 调用WindowOperator.processElement(record) 得到这条数据  streamOperator.processElement(record);

```
public boolean processInput() throws Exception {
		if (isFinished) {
			return false;
		}
		if (numRecordsIn == null) {
			try {
				numRecordsIn = ((OperatorMetricGroup) streamOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				numRecordsIn = new SimpleCounter();
			}
		}

		while (true) {
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrMark = deserializationDelegate.getInstance();

					if (recordOrMark.isWatermark()) {
						// handle watermark
						statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), currentChannel);
						continue;
					} else if (recordOrMark.isStreamStatus()) {
						// handle stream status
						statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), currentChannel);
						continue;
					} else if (recordOrMark.isLatencyMarker()) {
						// handle latency marker
						synchronized (lock) {
							streamOperator.processLatencyMarker(recordOrMark.asLatencyMarker());
						}
						continue;
					} else {
						// now we can do the actual processing
						StreamRecord<IN> record = recordOrMark.asRecord();
						synchronized (lock) {
							numRecordsIn.inc();
							streamOperator.setKeyContextElement1(record);
							streamOperator.processElement(record);
						}
						return true;
					}
				}
			}

			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
			if (bufferOrEvent != null) {
				if (bufferOrEvent.isBuffer()) {
					currentChannel = bufferOrEvent.getChannelIndex();
					currentRecordDeserializer = recordDeserializers[currentChannel];
					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
				}
				else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}
				}
			}
			else {
				isFinished = true;
				if (!barrierHandler.isEmpty()) {
					throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
				}
				return false;
			}
		}
	}
```


### WindowOperator.processElement(record)
- 分配Window,也就是计算Window的开启时间和结束时间，该计算方式，算法，恰好把所有元素都分配到整size的时间段
    如size = 5秒，5000毫秒,也就是时间戳在这些范围内的，就会被分配的最近的一个区间
    ```
    [2019-03-06 20:00:00   -> 2019-03-06 20:00:05)
    [2019-03-06 20:00:05   -> 2019-03-06 20:00:10)
    [2019-03-06 20:00:15   -> 2019-03-06 20:00:20)
    [2019-03-06 20:00:20   -> 2019-03-06 20:00:25)
    [2019-03-06 20:00:25   -> 2019-03-06 20:00:30)
    ......
    ```
    - 开始时间(单位:毫秒),此处offset = 0,timestamp = AssignerWithPeriodicWatermarks.extractTimestamp() 提取的时间戳
    ```
    long start = timestamp - (timestamp - offset + windowSize) % windowSize;
    ```
    
    - 结束时间(单位:毫秒),size 为.timeWindowAll(Time.seconds(5)),5秒即为5000 毫秒
    ```
    long end = start + size 
    ```

    ```
    	final Collection<W> elementWindows = windowAssigner.assignWindows(
			element.getValue(), element.getTimestamp(), windowAssignerContext);
    ```
- 如果当前元素的时间戳，计算后，结束时间大于该Window的结束时间，就说明划到后面的window去了，也就是这一条数据延时了
```
		// drop if the window is already late
				if (isWindowLate(window)) {
					continue;
				}
```
- windowState为HeapListState来存储元素
```
windowState.add(element.getValue());
```

- 调用trigger函数，就是给每个window绑定触发器，当window到了结束时间，就会被触发，不过他不是每个元素触发一次，而是按key,进行了Set去重，每个key会调一次WindowOperator.onProcessingTime()函数
```
TriggerResult triggerResult = triggerContext.onElement(element);

```



```
public void processElement(StreamRecord<IN> element) throws Exception {
		final Collection<W> elementWindows = windowAssigner.assignWindows(
			element.getValue(), element.getTimestamp(), windowAssignerContext);

		//if element is handled by none of assigned elementWindows
		boolean isSkippedElement = true;

		final K key = this.<K>getKeyedStateBackend().getCurrentKey();

		if (windowAssigner instanceof MergingWindowAssigner) {
			MergingWindowSet<W> mergingWindows = getMergingWindowSet();

			for (W window: elementWindows) {

				// adding the new window might result in a merge, in that case the actualWindow
				// is the merged window and we work with that. If we don't merge then
				// actualWindow == window
				W actualWindow = mergingWindows.addWindow(window, new MergingWindowSet.MergeFunction<W>() {
					@Override
					public void merge(W mergeResult,
							Collection<W> mergedWindows, W stateWindowResult,
							Collection<W> mergedStateWindows) throws Exception {

						if ((windowAssigner.isEventTime() && mergeResult.maxTimestamp() + allowedLateness <= internalTimerService.currentWatermark())) {
							throw new UnsupportedOperationException("The end timestamp of an " +
									"event-time window cannot become earlier than the current watermark " +
									"by merging. Current watermark: " + internalTimerService.currentWatermark() +
									" window: " + mergeResult);
						} else if (!windowAssigner.isEventTime() && mergeResult.maxTimestamp() <= internalTimerService.currentProcessingTime()) {
							throw new UnsupportedOperationException("The end timestamp of a " +
									"processing-time window cannot become earlier than the current processing time " +
									"by merging. Current processing time: " + internalTimerService.currentProcessingTime() +
									" window: " + mergeResult);
						}

						triggerContext.key = key;
						triggerContext.window = mergeResult;

						triggerContext.onMerge(mergedWindows);

						for (W m: mergedWindows) {
							triggerContext.window = m;
							triggerContext.clear();
							deleteCleanupTimer(m);
						}

						// merge the merged state windows into the newly resulting state window
						windowMergingState.mergeNamespaces(stateWindowResult, mergedStateWindows);
					}
				});

				// drop if the window is already late
				if (isWindowLate(actualWindow)) {
					mergingWindows.retireWindow(actualWindow);
					continue;
				}
				isSkippedElement = false;

				W stateWindow = mergingWindows.getStateWindow(actualWindow);
				if (stateWindow == null) {
					throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
				}

				windowState.setCurrentNamespace(stateWindow);
				windowState.add(element.getValue());

				triggerContext.key = key;
				triggerContext.window = actualWindow;

				TriggerResult triggerResult = triggerContext.onElement(element);

				if (triggerResult.isFire()) {
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					emitWindowContents(actualWindow, contents);
				}

				if (triggerResult.isPurge()) {
					windowState.clear();
				}
				registerCleanupTimer(actualWindow);
			}

			// need to make sure to update the merging state in state
			mergingWindows.persist();
		} else {
			for (W window: elementWindows) {

				// drop if the window is already late
				if (isWindowLate(window)) {
					continue;
				}
				isSkippedElement = false;

				windowState.setCurrentNamespace(window);
				windowState.add(element.getValue());

				triggerContext.key = key;
				triggerContext.window = window;

				TriggerResult triggerResult = triggerContext.onElement(element);

				if (triggerResult.isFire()) {
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					emitWindowContents(window, contents);
				}

				if (triggerResult.isPurge()) {
					windowState.clear();
				}
				registerCleanupTimer(window);
			}
		}

		// side output input event if
		// element not handled by any window
		// late arriving tag has been set
		// windowAssigner is event time and current timestamp + allowed lateness no less than element timestamp
		if (isSkippedElement && isElementLate(element)) {
			if (lateDataOutputTag != null){
				sideOutput(element);
			} else {
				this.numLateRecordsDropped.inc();
			}
		}
	}

```


### WindowOperator.onEventTime()
- 当自定义程序datastream.assignTimestampsAndWatermarks.AssignerWithPeriodicWatermarks.getCurrentWatermark得到的值，大于当前window的end，说明当前window可以结束了，就会触发调用 WindowOperator.onEventTime()
- 	triggerResult.isFire()//就会满足条件
- windowState.get();//取出当前window中开始时间到结束时间的，所有元素
- 调用emitWindowContents()函数进行处理

```
public void onEventTime(InternalTimer<K, W> timer) throws Exception {
		triggerContext.key = timer.getKey();
		triggerContext.window = timer.getNamespace();

		MergingWindowSet<W> mergingWindows;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
			if (stateWindow == null) {
				// Timer firing for non-existent window, this can only happen if a
				// trigger did not clean up timers. We have already cleared the merging
				// window and therefore the Trigger state, however, so nothing to do.
				return;
			} else {
				windowState.setCurrentNamespace(stateWindow);
			}
		} else {
			windowState.setCurrentNamespace(triggerContext.window);
			mergingWindows = null;
		}

		TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

		if (triggerResult.isFire()) {
			ACC contents = windowState.get();
			if (contents != null) {
				emitWindowContents(triggerContext.window, contents);
			}
		}

		if (triggerResult.isPurge()) {
			windowState.clear();
		}

		if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
			clearAllState(triggerContext.window, windowState, mergingWindows);
		}

		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}

```

### WindowOperator.emitWindowContents
- userFunction 调用InternalIterableProcessAllWindowFunction.process()


```
	private void emitWindowContents(W window, ACC contents) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
		processContext.window = window;
		userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);
	}

```

### InternalIterableProcessAllWindowFunction.process
- wrappedFunction.process为用户定义的处理函数  AllWindowedStream.process()函数，即自己定义的处理window的函数
- input 为当前widow的所有元素
- 调用TimestampedCollector.collect()一条一条元素发送

```
	public void process(Byte key, final W window, final InternalWindowContext context, Iterable<IN> input, Collector<OUT> out) throws Exception {
		this.ctx.window = window;
		this.ctx.internalContext = context;
		ProcessAllWindowFunction<IN, OUT, W> wrappedFunction = this.wrappedFunction;
		wrappedFunction.process(ctx, input, out);
	}

```

### AbstractStreamOperator.CountingOutput
- 调用CopyingChainingOutput.collect()

```
public void collect(StreamRecord<OUT> record) {
			numRecordsOut.inc();
			output.collect(record);
		}
```
### Window(StreamSink)


### OperatorChain.CopyingChainingOutput
- 调用StreamSink.processElement()
```
protected <X> void pushToOperator(StreamRecord<X> record) {
			try {
				// we know that the given outputTag matches our OutputTag so the record
				// must be of the type that our operator (and Serializer) expects.
				@SuppressWarnings("unchecked")
				StreamRecord<T> castRecord = (StreamRecord<T>) record;

				numRecordsIn.inc();
				StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
				operator.setKeyContextElement1(copy);
				operator.processElement(copy);
			} catch (ClassCastException e) {
				if (outputTag != null) {
					// Enrich error message
					ClassCastException replace = new ClassCastException(
						String.format(
							"%s. Failed to push OutputTag with id '%s' to operator. " +
								"This can occur when multiple OutputTags with different types " +
								"but identical names are being used.",
							e.getMessage(),
							outputTag.getId()));

					throw new ExceptionInChainedOperatorException(replace);
				} else {
					throw new ExceptionInChainedOperatorException(e);
				}
			} catch (Exception e) {
				throw new ExceptionInChainedOperatorException(e);
			}

		}
	}

```

### StreamSink.processElement
- 调用PrintSinkFuntion.invoke 输出当前元素

```
	public void processElement(StreamRecord<IN> element) throws Exception {
		sinkContext.element = element;
		userFunction.invoke(element.getValue(), sinkContext);
	}
```




---