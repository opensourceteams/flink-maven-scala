package com.opensourceteams.module.bigdata.flink.example.dataset.worldcount

import org.apache.flink.api.scala.ExecutionEnvironment


/**
  * 批处理，DataSet WordCount分析
  */
object WordCountTestRun {


  def main(args: Array[String]): Unit = {

    val env : ExecutionEnvironment= ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.readTextFile("file:/opt/n_001_workspaces/bigdata/flink/flink-maven-scala-2/src/main/resources/data/line.txt")


    import org.apache.flink.streaming.api.scala._
    dataSet.flatMap(x => x.split(" ")).map((_,1)).groupBy(0).sum(1)

      .print()


  }

}
