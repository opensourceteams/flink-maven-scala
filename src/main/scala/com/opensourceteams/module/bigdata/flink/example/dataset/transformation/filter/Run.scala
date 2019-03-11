package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.filter

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  * filter 过滤器，对数据进行过滤处理
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements("c a b d a    c","d c   a b c d")


    val dataSet2 = dataSet.flatMap(_.toUpperCase().split(" ")).filter(_.nonEmpty)
    dataSet2.print()

  }

}
