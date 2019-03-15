package com.opensourceteams.module.bigdata.flink.example.sql.dataset.from

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements(("小明",15,"男"),("小李",25,"女"))

    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user2",dataSet,'name,'age,'sex)



    tableEnv.sqlQuery(s"select name,age FROM user2")
      .first(100).print()


  }

}
