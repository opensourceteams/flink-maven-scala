package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.cogroup

import java.lang

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",1),("g",1),("a",1))
    val dataSet2 = env.fromElements(("a",1),("f",1))


    //全外连接
    val dataSet3 = dataSet.coGroup(dataSet2).where(0).equalTo(0)
    {
      new CoGroupFunction[(String,Int),(String,Int), Collector[(String,Int)]] {
        override def coGroup(first: lang.Iterable[(String, Int)], second: lang.Iterable[(String, Int)], out: Collector[Collector[(String, Int)]]): Unit = {
          println("==============开始")
          println("first")
          println(first)
          val iteratorFirst = first.iterator()
          while (iteratorFirst.hasNext()){
            println(iteratorFirst.next())
          }

          println("second")
          println(second)
          val iteratorSecond = second.iterator()
          while (iteratorSecond.hasNext()){
            println(iteratorSecond.next())
          }
          println("==============结束")

        }
      }
    }


    dataSet3.print()

  }

}
