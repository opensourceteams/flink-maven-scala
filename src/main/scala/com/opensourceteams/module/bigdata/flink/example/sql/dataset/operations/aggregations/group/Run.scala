package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.aggregations.group

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements(("小明",15,"男",1500),("小王",45,"男",4000),("小李",25,"女",800),("小慧",35,"女",500))

    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user1",dataSet,'name,'age,'sex,'salary)



    //汇总所有数据
    tableEnv.sqlQuery(s"select sex,sum(salary) FROM user1 group by sex")
      .first(100).print()

    /**
      * 输出结果
      *
      * 女,1300
      * 男,5500
      */

  }

}
