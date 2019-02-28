package com.opensourceteams.module.bigdata.flink.example.stream.worldcount.nc

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


    val configuration : Configuration = new Configuration()
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
      .timeWindow(Time.seconds(20))
      //.countWindow(3)
      //.countWindow(3,1)
      //.countWindowAll(3)


      .sum("count" )

    textResult.print().setParallelism(1)



    if(args == null || args.size ==0){
      env.execute("默认作业")

      //执行计划
      //println(env.getExecutionPlan)
      //StreamGraph
     //println(env.getStreamGraph.getStreamingPlanAsJSON)



      //JsonPlanGenerator.generatePlan(jobGraph)

    }else{
      env.execute(args(0))
    }

    println("结束")

  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}
