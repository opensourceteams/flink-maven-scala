package com.opensourceteams.module.bigdata.flink.example.dataset.worldcount

import org.apache.flink.api.scala.ExecutionEnvironment

object WorldCountTest {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val dataSet = env.fromElements("hello is   flink flink verry good","flink  flink is ok")
   // val result = dataSet.flatMap(_.toLowerCase.split(" ")).filter(_.nonEmpty).map((_,1))
      //  .groupBy(0).sum(1)

    val result = dataSet.flatMap(_.toLowerCase().split(" ")).filter(_.nonEmpty).map((_,1)).groupBy(0).sum(1)
    result.print()


  }

}
