package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.setOperations.intersect

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements((1,"小明",15,"男",1500),(2,"小王",45,"男",4000),(3,"小李",25,"女",800),(4,"小慧",35,"女",500))
    val dataSet2 = env.fromElements((1,"小明",15,"男",1500),(2,"小王",45,"男",4000),(30,"小李",25,"女",800),(40,"小慧",35,"女",500))

    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user",dataSet,'id,'name,'age,'sex,'salary)
    tableEnv.registerDataSet("t2",dataSet2,'id,'name,'age,'sex,'salary)


    /**
      *  INTERSECT 连接两个表,找相同的数据(相交的数据，重叠的数据)
      */
    tableEnv.sqlQuery(
      "select * from ("
                +"select t1.* FROM `user` as t1 ) " +
                + " INTERSECT "
                + " ( select t2.* FROM t2 )"



       )
      .first(100).print()


    /**
      * 输出结果
      *
      * 1,小明,15,男,1500
      * 2,小王,45,男,4000
      *
      */

  }

}
