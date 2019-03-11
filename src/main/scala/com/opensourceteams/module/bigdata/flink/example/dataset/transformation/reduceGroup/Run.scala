package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.reduceGroup

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
  * 相当于，直接把前边的集合，全部做为集合参数传进来
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(3,5,8,9)

    dataSet.reduceGroup(x => {
      for(e <- x.toList) println(e)
    })


  }

}
