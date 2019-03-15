package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.aggregations.distinct_many

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements(("a",15,"male"),("a",15,"female"),("d",25,"male"),("a",35,"female"))

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    tableEnv.registerDataSet("user1",dataSet,'name,'age,'sex)


    /**
      * name,age两列都相等，才会去重
      */
    tableEnv.sqlQuery("select distinct name,age  FROM user1   ")
      .first(100).print()


  }

}
