package com.opensourceteams.module.bigdata.flink.example.stream.operator.keyby

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * nc -lk 1234  输入数据
  */
object Run {

  def main(args: Array[String]): Unit = {


    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream("localhost", port, '\n')


    val dataStream2 = dataStream.flatMap(x => x.split(" ")).map((_,1))
      .keyBy(0)

      //dataStream.keyBy("someKey") // Key by field "someKey"
      //dataStream.keyBy(0) // Key by the first element of a Tuple

      .timeWindow(Time.seconds(2))//每2秒滚动窗口
      .sum(1)





    dataStream2.print()



    if(args == null || args.size ==0){
      env.execute("默认作业")
    }else{
      env.execute(args(0))
    }

    println("结束")

  }


}
