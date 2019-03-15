package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.limit

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)


    val dataSet = env.fromElements(("小明",15,"男"),("小王",45,"男"),("小李",25,"女"),("小慧",35,"女"))

    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user1",dataSet,'name,'age,'sex)


    /**
      * 先排序，按age的降序排序，输出前100位结果,注意是按同一个并行度中的数据进行排序，也就是同一个分区
      */
    tableEnv.sqlQuery(s"select name,age FROM user1  ORDER BY age desc LIMIT 100  ")
      .first(100).print()


    /**
      * 输出结果 并行度设置为2
      *
      * 小明,15
      * 小王,45
      * 小慧,35
      * 小李,25
      */


  }

}
