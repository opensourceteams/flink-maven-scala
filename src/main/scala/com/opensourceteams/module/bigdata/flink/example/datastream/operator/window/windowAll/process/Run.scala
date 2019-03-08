package com.opensourceteams.module.bigdata.flink.example.datastream.operator.window.windowAll.process

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * nc -lk 1234  输入数据
  */
object Run {

  def main(args: Array[String]): Unit = {


    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)  //设置并行度
    val dataStream = env.socketTextStream("localhost", port, '\n')


    dataStream.flatMap(x => x.split(" ")).map((_,1))
      .keyBy(0)


      .windowAll( TumblingProcessingTimeWindows.of(Time.seconds(2)))
        .process(new ProcessAllWindowFunction[(String, Int),(String, Int),TimeWindow] {
          override def process(context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
            //可以对当前window中的所有元素进行操作，处理后，再发送给Sink
            for(element <- elements) out.collect(element)
          }
        })

        .print()





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
