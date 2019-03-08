package com.opensourceteams.module.bigdata.flink.example.datastream.worldcount.nc.parallelism

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * nc -lk 1234  输入数据
  */
object SocketWindowWordCountLocal {



  def main(args: Array[String]): Unit = {


    val port = 1234
    // get the execution environment
   // val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val configuration : Configuration = getConfiguration(true)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1,configuration)





    // get input data by connecting to the socket
    val dataStream = env.socketTextStream("localhost", port, '\n')



    import org.apache.flink.streaming.api.scala._
    val textResult = dataStream.flatMap( w => w.split("\\s") ).map( w => WordWithCount(w,1))
      .keyBy("word")
      /**
        * 每20秒刷新一次，相当于重新开始计数，
        * 好处，不需要一直拿所有的数据统计
        * 只需要在指定时间间隔内的增量数据，减少了数据规模
        */
      .timeWindow(Time.seconds(5))
      //.countWindow(3)
      //.countWindow(3,1)
      //.countWindowAll(3)


      .sum("count" )

    textResult
      .setParallelism(3)
      .print()




    if(args == null || args.size ==0){


      println("==================================以下为执行计划==================================")
      println("执行地址(firefox效果更好):https://flink.apache.org/visualizer")
      //执行计划
      //println(env.getExecutionPlan)
     // println("==================================以上为执行计划 JSON串==================================\n")
      //StreamGraph
     println(env.getStreamGraph.getStreamingPlanAsJSON)



      //JsonPlanGenerator.generatePlan(jobGraph)

      env.execute("默认作业")

    }else{
      env.execute(args(0))
    }

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
