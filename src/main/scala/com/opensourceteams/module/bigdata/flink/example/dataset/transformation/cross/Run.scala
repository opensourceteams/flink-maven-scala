package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.cross

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",1),("g",1),("f",1))
    val dataSet2 = env.fromElements(("d",1),("f",1),("g",1),("f",1))


    //全外连接
    val dataSet3 = dataSet.cross(dataSet2)



    dataSet3.print()

  }

}
