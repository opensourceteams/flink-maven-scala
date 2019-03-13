package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.first

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",1),("g",1),("f",1))


    //全外连接
    val dataSet3 = dataSet.first(3)



    dataSet3.print()

  }

}
