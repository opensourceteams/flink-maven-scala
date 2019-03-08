package com.opensourceteams.module.bigdata.flink.example.stream.operator.flatmap

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
  * nc -lk 1234  输入数据
  */
object Run {

  def main(args: Array[String]): Unit = {


    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream("localhost", port, '\n')

    val dataStream2 = dataStream.flatMap(x => x.split(" "))

    dataStream2.print()



    if(args == null || args.size ==0){
      env.execute("默认作业")
    }else{
      env.execute(args(0))
    }

    println("结束")

  }


}
