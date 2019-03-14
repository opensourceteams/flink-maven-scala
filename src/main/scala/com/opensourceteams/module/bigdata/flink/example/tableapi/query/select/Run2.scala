package com.opensourceteams.module.bigdata.flink.example.tableapi.query.select

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sources.CsvTableSource

object Run2 {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //创建CsvTableSource
    val csvTableSource = CsvTableSource.builder()
      .path("src/main/resources/data/csv/user.csv")
      .ignoreFirstLine()
      .fieldDelimiter(",")
      .field("name", Types.STRING)
      .field("age", Types.INT)
      .field("sex", Types.STRING)
      .build()


    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerTableSource("user",csvTableSource)




    //查询表数据, select 选择需要的列
    tableEnv.scan("user").select('name,'age,'sex)

      .first(3).print()
  }

}
