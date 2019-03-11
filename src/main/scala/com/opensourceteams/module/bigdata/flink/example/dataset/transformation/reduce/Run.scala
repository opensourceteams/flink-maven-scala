package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.reduce

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
  * 相当于进行所有元素的累加操作，求和操作
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(3,5,8,9)
    //  3 + 5 + 8 + 9


    val dataSet2 = dataSet.reduce((a,b) => {
      println(s"${a} + ${b} = ${a +b}")
      a + b
    })
    dataSet2.print()

  }

}
