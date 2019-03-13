package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.join

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",1),("g",1),("f",1))
    val dataSet2 = env.fromElements(("d",1),("f",1),("g",1),("f",1))


    //全外连接
    val dataSet3 = dataSet.join(dataSet2).where(0).equalTo(0)



    dataSet3.print()

  }

}
