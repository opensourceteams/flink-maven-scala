package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.aggregate.maxBy

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",1),("g",1),("f",1))

    /**
      * (a,1)
      * (b,1)
      * (c,1)
      * (a,1)
      * (c,1)
      * (d,1)
      * (f,1)
      * (g,1)
      */

      //求哪一列的最小值
    val dataSet2 = dataSet.groupBy(0).maxBy(1)



    dataSet2.print()

  }

}
