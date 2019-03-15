package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.join.fullOuterJoin

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements((1,"小明",15,"男",1500),(2,"小王",45,"男",4000),(3,"小李",25,"女",800),(4,"小慧",35,"女",500))
    val dataSetGrade = env.fromElements((1,"语文",100),(2,"数学",80),(1,"外语",50),(10,"外语",90) )

    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user",dataSet,'id,'name,'age,'sex,'salary)
    tableEnv.registerDataSet("grade",dataSetGrade,'userId,'name,'fraction)



  //左，右，全匹配所有数据
    tableEnv.sqlQuery("select `user`.*,grade.name,grade.fraction FROM `user` FULL OUTER JOIN  grade on  `user`.id = grade.userId ")
      .first(100).print()


    /**
      * 输出结果
      *
      *
      * 3,小李,25,女,800,null,null
      * 1,小明,15,男,1500,外语,50
      * 1,小明,15,男,1500,语文,100
      * 2,小王,45,男,4000,数学,80
      * 4,小慧,35,女,500,null,null
      * null,null,null,null,null,外语,90
      *
      *
      *
      */

  }

}
