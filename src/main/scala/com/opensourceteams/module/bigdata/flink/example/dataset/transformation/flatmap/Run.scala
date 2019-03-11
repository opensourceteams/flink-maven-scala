package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.flatmap

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements("c a b d a c","d c a b c d")


    val dataSet2 = dataSet.flatMap(_.toUpperCase().split(" "))
    dataSet2.print()

  }

}
