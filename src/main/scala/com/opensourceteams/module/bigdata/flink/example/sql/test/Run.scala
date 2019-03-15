package com.opensourceteams.module.bigdata.flink.example.sql.test

import java.sql.{Date, Timestamp}

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements(
      ("小明",15,"男",Date.valueOf("2000-03-01")),
      ("小王",45,"男",Date.valueOf("2000-04-01")),
      ("小李",25,"女",Date.valueOf("2000-05-01")),
      ("小慧",35,"女",Date.valueOf("2000-06-01")))

    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user",dataSet,'name,'age,'sex,'birthday)



    tableEnv.sqlQuery(s"select u.name,u.age,u.sex,u.birthday FROM `user` as u   ")
      .first(100).print()


    /**
      * 结果
      *
      * 小李,25,女
      * 小慧,35,女
      */

  }

}
