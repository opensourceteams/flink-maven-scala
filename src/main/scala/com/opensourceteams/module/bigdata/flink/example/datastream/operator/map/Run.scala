package com.opensourceteams.module.bigdata.flink.example.datastream.operator.map

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import org.apache.flink.streaming.api.scala._

/**
  * nc -lk 1234  输入数据
  */
object Run {

  def main(args: Array[String]): Unit = {


    val port = 1234
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream("localhost", port, '\n')

    val dataStreamMap = dataStream.map(x => x + " 增加的数据")

    dataStreamMap.print()


    if(args == null || args.size ==0){
      env.execute("默认作业")
    }else{
      env.execute(args(0))
    }

    println("结束")

  }

}
