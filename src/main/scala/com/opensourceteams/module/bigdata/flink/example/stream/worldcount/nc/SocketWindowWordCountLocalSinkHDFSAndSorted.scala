package com.opensourceteams.module.bigdata.flink.example.stream.worldcount.nc

import java.time.ZoneId

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * nc -lk 1234  输入数据
  */
object SocketWindowWordCountLocalSinkHDFSAndSorted {


  def getConfiguration(isDebug:Boolean = false):Configuration={

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

  def main(args: Array[String]): Unit = {


    val port = 1234
    // get the execution environment
   // val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val configuration : Configuration = getConfiguration(true)

    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1,configuration)






    // get input data by connecting to the socket
    val dataStream = env.socketTextStream("localhost", port, '\n')



    import org.apache.flink.streaming.api.scala._
    val dataStreamDeal = dataStream.flatMap( w => w.split("\\s") ).map( w => WordWithCount(w,1))
      .keyBy("word")
      /**
        * 每20秒刷新一次，相当于重新开始计数，
        * 好处，不需要一直拿所有的数据统计
        * 只需要在指定时间间隔内的增量数据，减少了数据规模
        */
      .timeWindow(Time.seconds(30))

      //.sum("count" )
      //同一个window中每个key发一次
      .process(new ProcessWindowFunction[WordWithCount,WordWithCount,Tuple,TimeWindow] () {
          override def process(key: Tuple, context: Context, elements: Iterable[WordWithCount], out: Collector[WordWithCount]): Unit = {
            // 空列表
            val listBuffer = new ListBuffer[WordWithCount]
            listBuffer.appendAll(elements)
            val sortedListBuffer = listBuffer.sortWith((a,b) => a.word.compareTo(b.word) < 0 )
            for(wordCount <- sortedListBuffer)  out.collect(wordCount)
          }
      } )


      //.countWindow(3)
      //.countWindow(3,1)
      //.countWindowAll(3)




    //textResult.print().setParallelism(1)

    val bucketingSink = new BucketingSink[WordWithCount]("file:/opt/n_001_workspaces/bigdata/flink/flink-maven-scala-2/sink-data")


    bucketingSink.setBucketer(new DateTimeBucketer[WordWithCount]("yyyy-MM-dd--HHmm", ZoneId.of("Asia/Shanghai")))
    //bucketingSink.setWriter(new SequenceFileWriter[IntWritable, Text]())
    //bucketingSink.setWriter(new SequenceFileWriter[WordWithCount]())
    //bucketingSink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    //bucketingSink.setBatchSize(100 ) // this is 400 MB,
    bucketingSink.setBatchSize(1024 * 1024 * 400 ) // this is 400 MB,
    //bucketingSink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins
    bucketingSink.setBatchRolloverInterval( 2 * 1000); // this is 20 mins
    //setInactiveBucketCheckInterval
    //setInactiveBucketThreshold
    //每间隔多久时间，往Sink中写数据，不是每天条数据就写，浪费资源
    bucketingSink.setInactiveBucketCheckInterval(2 * 1000)
    bucketingSink.setInactiveBucketThreshold(2 * 1000)
   // bucketingSink.setAsyncTimeout(1 * 1000)


    dataStreamDeal.setParallelism(1)
      .addSink(bucketingSink)




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

/*  abstract private   class OrderWindowFunction extends ProcessWindowFunction<WordWithCount,WordWithCount,WordWithCount,TimeWindow> {

  }*/
}


