package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.fullOuterJoin

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",2),("g",5))
    val dataSet2 = env.fromElements(("g",1),("f",1))


    //全外连接
    val dataSet3 = dataSet.fullOuterJoin(dataSet2).where(0).equalTo(0){
      (x,y) => {
        var countY = 0;
        if(y != null ){
          countY = y._2
        }


        var countX = 0;
        if(x != null ){
          countX = x._2
        }
        (x._1,countX + countY)
      }
    }




    dataSet3.print()

  }

}
