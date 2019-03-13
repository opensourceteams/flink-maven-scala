package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.combineGroup

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector



/**
  * 相同的key的元素，都一次做为参数传进来了
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataSet = env.fromElements("a","a","c","b","a")



    /**
      * 中间数据
      * (a,1)
      * (a,1)
      * (c,1)
      * (b,1)
      * (a,1)
      */
    val result = dataSet.map((_,1)).groupBy(0).combineGroup(


      (in, out: Collector[(String,Int)]) =>{

        var count = 0 ;
        var word = "";
        while (in.hasNext){

          val next  = in.next()
          word = next._1
          count = count + next._2

        }
        out.collect((word,count))
      }


    )


    result.print()


  }

}
