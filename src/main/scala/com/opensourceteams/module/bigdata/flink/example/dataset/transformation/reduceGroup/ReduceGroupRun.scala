package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.reduceGroup

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.api.scala.extensions._
import org.apache.flink.util.Collector


/**
  * 相当于，直接把前边的集合，全部做为集合参数传进来
  */
object ReduceGroupRun {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements("a","a","c","b","a")

    /**
      * 中间数据
      * (a,1)
      * (a,1)
      * (c,1)
      * (b,1)
      * (a,1)
      */
    val result = dataSet.map((_,1)).groupBy(0).reduceGroup(

      (in, out: Collector[(String,Int)]) =>
        in.toSet foreach (out.collect)

    )


    result.collect()


  }

}
