package com.opensourceteams.module.bigdata.flink.example.tableapi.test

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment

object Run {



  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )
    val dataSet2 = env.fromElements( (1,"a",100),(20,"b",20), (30,"c",30) )



    //列不能重复
    val table = tableEnv.fromDataSet(dataSet,'a,'b,'c)
    val table2 = tableEnv.fromDataSet(dataSet2,'d,'e,'f)



    table.join(table2,"a = d").first(1000).print()


    /**
      * 输出结果
      *
      *
      * null,null,null,20,b,20
      * null,null,null,30,c,30
      * 1,a,10,1,a,100
      */



  }

}
