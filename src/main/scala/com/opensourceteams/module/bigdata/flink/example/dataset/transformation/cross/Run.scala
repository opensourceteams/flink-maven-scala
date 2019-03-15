package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.cross

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",1),("g",1),("f",1))
    val dataSet2 = env.fromElements(("d",1),("f",1),("g",1),("f",1))


    //全连接,拿表1的每一个元素，去连接表二中的每一个元素
    val dataSet3 = dataSet.cross(dataSet2)



    dataSet3.print()


    /**
      * 输出结果
      *
      * ((a,1),(d,1))
      * ((a,1),(f,1))
      * ((a,1),(g,1))
      * ((a,1),(f,1))
      * ((g,1),(d,1))
      * ((g,1),(f,1))
      * ((g,1),(g,1))
      * ((g,1),(f,1))
      * ((f,1),(d,1))
      * ((f,1),(f,1))
      * ((f,1),(g,1))
      * ((f,1),(f,1))
      */

  }

}
