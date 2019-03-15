# Flink1.7.2 sql 批处理示例

## 源码
- https://github.com/opensourceteams/flink-maven-scala

## 概述
- 本文为Flink sql Dataset 示例 
- 主要操作包括:


## SELECT

### Scan / Select
- 功能描述: 查询一个表中的所有数据
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.scan

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements(("小明",15,"男"),("小王",45,"男"),("小李",25,"女"),("小慧",35,"女"))

    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user1",dataSet,'name,'age,'sex)



    tableEnv.sqlQuery(s"select name,age FROM user1")
      .first(100).print()


    /**
      * 输出结果
      *
      * 小明,15
      * 小王,45
      * 小李,25
      * 小慧,35
      */
  }

}


```

- 输出结果

```aidl
小明,15
小王,45
小李,25
小慧,35

```




### as (table)
- 功能描述: 给表名取别称
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.scan

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements(("小明",15,"男"),("小王",45,"男"),("小李",25,"女"),("小慧",35,"女"))

    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user1",dataSet,'name,'age,'sex)



    tableEnv.sqlQuery(s"select t1.name,t1.age FROM user1 as t1")
      .first(100).print()


    /**
      * 输出结果
      *
      * 小明,15
      * 小王,45
      * 小李,25
      * 小慧,35
      */
  }

}


```

- 输出结果

```aidl
小明,15
小王,45
小李,25
小慧,35

```




### as (column)
- 功能描述: 给表名取别称
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.scan

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements(("小明",15,"男"),("小王",45,"男"),("小李",25,"女"),("小慧",35,"女"))

    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user1",dataSet,'name,'age,'sex)



    tableEnv.sqlQuery(s"select name a,age as b FROM user1 ")
      .first(100).print()


    /**
      * 输出结果
      *
      * 小明,15
      * 小王,45
      * 小李,25
      * 小慧,35
      */
  }

}


```

- 输出结果

```aidl
小明,15
小王,45
小李,25
小慧,35

```

### limit
- 功能描述:查询一个表的数据，只返回指定的前几行(争对并行度而言,所以并行度不一样，结果不一样)

- scala 程序

```aidl
package com.opensourceteams.mo`dule.bigdata.flink.example.sql.dataset.operations.limit

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

    /**
      * 输出结果 并行度设置为1
      *
      * 小王,45
      * 小慧,35
      * 小李,25
      * 小明,15
      */



  }

}

```

- 输出结果

```aidl
小明,15
小王,45
小慧,35
小李,25
```


### Where / Filter
- 功能描述:列加条件过滤表中的数据
- scala 程序

```aidl
package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.where

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements(("小明",15,"男"),("小王",45,"男"),("小李",25,"女"),("小慧",35,"女"))

    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user1",dataSet,'name,'age,'sex)



    tableEnv.sqlQuery(s"select name,age,sex FROM user1 where sex = '女'")
      .first(100).print()


    /**
      * 输出结果
      * 
      * 小李,25,女
      * 小慧,35,女
      */
    
  }

}


```

- 输出结果

```aidl

小李,25,女
小慧,35,女
```





### between and (where) 
- 功能描述: 过滤列中的数据,  开始数据  <= data  <= 结束数据
- scala 程序

```aidl
package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.whereBetweenAnd

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements(("小明",15,"男"),("小王",45,"男"),("小李",25,"女"),("小慧",35,"女"))

    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user1",dataSet,'name,'age,'sex)



    tableEnv.sqlQuery(s"select name,age,sex FROM user1 where age between 20 and  35")
      .first(100).print()


    /**
      * 结果
      *
      * 小李,25,女
      * 小慧,35,女
      */

  }

}


```

- 输出结果

```aidl
小李,25,女
小慧,35,女

```





### Sum 
- 功能描述: 求和所有数据
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.aggregations.sum

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
    tableEnv.sqlQuery(s"select sum(salary) FROM user1")
      .first(100).print()


    /**
      * 输出结果
      *
      * 6800
      */


  }

}


```

- 输出结果

```aidl
6800

```






### max 
- 功能描述: 求最大值
- scala 程序

```aidl
package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.aggregations.max

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
    tableEnv.sqlQuery(s"select max(salary) FROM user1 ")
      .first(100).print()


    /**
      * 输出结果
      *
      * 4000
      */


  }

}


```

- 输出结果

```aidl
4000

```




### min 
- 功能描述: 求最小值
- scala 程序

```aidl
package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.aggregations.min

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



    tableEnv.sqlQuery(s"select min(salary) FROM user1 ")
      .first(100).print()


    /**
      * 输出结果
      *
      * 500
      */


  }

}


```

- 输出结果

```aidl
500

```



### sum (group by ) 
- 功能描述: 按性别分组求和
- scala 程序

```aidl

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


```

- 输出结果

```aidl
女,1300
男,5500

```



### group by having 
- 功能描述:
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.aggregations.group_having

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



    //分组统计，having是分组条件查询
    tableEnv.sqlQuery(s"select sex,sum(salary) FROM user1 group by sex having sum(salary) >1500")
      .first(100).print()

    /**
      * 输出结果
      * 
      * 
      */


  }

}


```

- 输出结果

```aidl
男,5500

```





### distinct 
- 功能描述: 去重一列或多列
- scala 程序

```aidl
package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.aggregations.distinct

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements(("a",15,"male"),("a",45,"female"),("d",25,"male"),("c",35,"female"))

    val tableEnv = TableEnvironment.getTableEnvironment(env)

    tableEnv.registerDataSet("user1",dataSet,'name,'age,'sex)


    /**
      * 对数据去重
      */
    tableEnv.sqlQuery("select distinct name  FROM user1   ")
      .first(100).print()


    /**
      * 输出结果
      *
      * a
      * c
      * d
      */

  }

}


```

- 输出结果

```aidl
a
c
d

```



## join

### INNER JOIN

- 功能描述: 连接两个表，按指定的列，两列都存在值才输出
- scala 程序

```aidl
package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.join.innerJoin

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements((1,"小明",15,"男",1500),(2,"小王",45,"男",4000),(3,"小李",25,"女",800),(4,"小慧",35,"女",500))
    val dataSetGrade = env.fromElements((1,"语文",100),(2,"数学",80),(1,"外语",50) )

    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user",dataSet,'id,'name,'age,'sex,'salary)
    tableEnv.registerDataSet("grade",dataSetGrade,'userId,'name,'fraction)



    //内连接，两个表
   // tableEnv.sqlQuery("select * FROM `user`  INNER JOIN  grade on  `user`.id = grade.userId ")
    tableEnv.sqlQuery("select `user`.*,grade.name,grade.fraction FROM `user`  INNER JOIN  grade on  `user`.id = grade.userId ")
      .first(100).print()


    /**
      * 输出结果
      * 2,小王,45,男,4000,数学,80
      * 1,小明,15,男,1500,语文,100
      * 1,小明,15,男,1500,外语,50
      */

  }

}


```

- 输出结果

```aidl
2,小王,45,男,4000,数学,80
1,小明,15,男,1500,语文,100
1,小明,15,男,1500,外语,50

```


### left join 
- 功能描述:连接两个表，按指定的列，左表中存在值就一定输出，右表如果不存在，就显示为空
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.join.leftJoin

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements((1,"小明",15,"男",1500),(2,"小王",45,"男",4000),(3,"小李",25,"女",800),(4,"小慧",35,"女",500))
    val dataSetGrade = env.fromElements((1,"语文",100),(2,"数学",80),(1,"外语",50) )

    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user",dataSet,'id,'name,'age,'sex,'salary)
    tableEnv.registerDataSet("grade",dataSetGrade,'userId,'name,'fraction)



  //左连接，拿左边的表中的每一行数据，去关联右边的数据，如果有相同的匹配数据，就都匹配出来，如果没有，就匹配一条，不过右边的数据为空
    tableEnv.sqlQuery("select `user`.*,grade.name,grade.fraction FROM `user`  LEFT JOIN  grade on  `user`.id = grade.userId ")
      .first(100).print()


    /**
      * 输出结果
      *
      * 1,小明,15,男,1500,语文,100
      * 1,小明,15,男,1500,外语,50
      * 2,小王,45,男,4000,数学,80
      * 4,小慧,35,女,500,null,null
      * 3,小李,25,女,800,null,null
      *
      *
      */

  }

}


```

- 输出结果

```aidl
1,小明,15,男,1500,语文,100
1,小明,15,男,1500,外语,50
2,小王,45,男,4000,数学,80
4,小慧,35,女,500,null,null
3,小李,25,女,800,null,null

```






### right join 
- 功能描述:连接两个表，按指定的列，右表中存在值就一定输出，左表如果不存在，就显示为空
- scala 程序

```aidl
package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.join.rightJoin

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



  //左连接，拿左边的表中的每一行数据，去关联右边的数据，如果有相同的匹配数据，就都匹配出来，如果没有，就匹配一条，不过右边的数据为空
    tableEnv.sqlQuery("select `user`.*,grade.name,grade.fraction FROM `user`  RIGHT JOIN  grade on  `user`.id = grade.userId ")
      .first(100).print()


    /**
      * 输出结果
      *
      * 1,小明,15,男,1500,外语,50
      * 1,小明,15,男,1500,语文,100
      * 2,小王,45,男,4000,数学,80
      * null,null,null,null,null,外语,90
      *

      *
      */

  }

}


```

- 输出结果

```aidl
1,小明,15,男,1500,外语,50
1,小明,15,男,1500,语文,100
2,小王,45,男,4000,数学,80
null,null,null,null,null,外语,90

```


### full outer join 
- 功能描述: 连接两个表，按指定的列，只要有一表中存在值就一定输出，另一表如果不存在就显示为空
- scala 程序

```aidl

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


```

- 输出结果

```aidl
3,小李,25,女,800,null,null
1,小明,15,男,1500,外语,50
1,小明,15,男,1500,语文,100
2,小王,45,男,4000,数学,80
4,小慧,35,女,500,null,null
null,null,null,null,null,外语,90

```


## Set Operations


### union 
- 功能描述: 连接两个表中的数据，会去重
- scala 程序

```aidl
package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.setOperations.union

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
      *  union 连接两个表,会去重
      */
    tableEnv.sqlQuery(
      "select * from ("
                +"select t1.* FROM `user` as t1 ) " +
                + " UNION "
                + " ( select t2.* FROM t2 )"



       )
      .first(100).print()


    /**
      * 输出结果
      *
      * 30,小李,25,女,800
      * 40,小慧,35,女,500
      * 2,小王,45,男,4000
      * 4,小慧,35,女,500
      * 3,小李,25,女,800
      * 1,小明,15,男,1500
      *
      */

  }

}


```

- 输出结果

```aidl
30,小李,25,女,800
40,小慧,35,女,500
2,小王,45,男,4000
4,小慧,35,女,500
3,小李,25,女,800
1,小明,15,男,1500

```



### unionAll 
- 功能描述: 连接两表中的数据，不会去重
- scala 程序

```aidl
package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.setOperations.unionAll

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
      *  union 连接两个表,不会去重
      */
    tableEnv.sqlQuery(
      "select * from ("
                +"select t1.* FROM `user` as t1 ) " +
                + " UNION ALL "
                + " ( select t2.* FROM t2 )"



       )
      .first(100).print()


    /**
      * 输出结果
      *
      * 1,小明,15,男,1500
      * 2,小王,45,男,4000
      * 3,小李,25,女,800
      * 4,小慧,35,女,500
      * 1,小明,15,男,1500
      * 2,小王,45,男,4000
      * 30,小李,25,女,800
      * 40,小慧,35,女,500
      *
      */

  }

}


```

- 输出结果

```aidl
1,小明,15,男,1500
2,小王,45,男,4000
3,小李,25,女,800
4,小慧,35,女,500
1,小明,15,男,1500
2,小王,45,男,4000
30,小李,25,女,800
40,小慧,35,女,500

```




### INTERSECT 
- 功能描述: INTERSECT 连接两个表,找相同的数据(相交的数据，重叠的数据)
- scala 程序

```aidl

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


```

- 输出结果

```aidl
 1,小明,15,男,1500
 2,小王,45,男,4000

```



### in 
- 功能描述:  子查询
- scala 程序

```aidl
package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.setOperations.in

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
      *  in ,子查询
      */
    tableEnv.sqlQuery(

                "select t1.* FROM `user` t1  where t1.id in " +
                        " (select t2.id from t2) "




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


```

- 输出结果

```aidl
 1,小明,15,男,1500
 2,小王,45,男,4000

```


### EXCEPT 
- 功能描述: EXCEPT 连接两个表,找不相同的数据(不相交的数据，不重叠的数据)
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.setOperations.except

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
      *  EXCEPT 连接两个表,找不相同的数据(不相交的数据，不重叠的数据)
      */
    tableEnv.sqlQuery(
      "select * from ("
                +"select t1.* FROM `user` as t1 ) " +
                + " EXCEPT "
                + " ( select t2.* FROM t2 )"



       )
      .first(100).print()


    /**
      * 输出结果
      *
      * 3,小李,25,女,800
      * 4,小慧,35,女,500
      *
      */

  }

}


```

- 输出结果

```aidl
 3,小李,25,女,800
 4,小慧,35,女,500

```



## DML

### insert into 
- 功能描述:将一个表中的数据(source)，插入到 csv文件中(sink)
- scala程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.sql.dataset.operations.insert

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.api.common.typeinfo.TypeInformation

object Run {



  def main(args: Array[String]): Unit = {


    //得到批环境
    val env = ExecutionEnvironment.getExecutionEnvironment


    val dataSet = env.fromElements(("小明",15,"男"),("小王",45,"男"),("小李",25,"女"),("小慧",35,"女"))


    //得到Table环境
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //注册table
    tableEnv.registerDataSet("user1",dataSet,'name,'age,'sex)




    // create a TableSink
    val csvSink = new CsvTableSink("sink-data/csv/a.csv",",",1,WriteMode.OVERWRITE);
    val fieldNames = Array("name", "age", "sex")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.INT, Types.STRING)
    tableEnv.registerTableSink("t2",fieldNames,fieldTypes,csvSink)


    tableEnv.sqlUpdate(s" insert into  t2 select name,age,sex FROM user1  ")


    env.execute()


    /**
      * 输出结果
      * a.csv
      *
      * 小明,15,男
      * 小王,45,男
      * 小李,25,女
      * 小慧,35,女
      */





  }

}


```

- 输出数据 a.csv
```aidl
小明,15,男
小王,45,男
小李,25,女
小慧,35,女


```



### Scan 
- 功能描述:
- scala 程序

```aidl

```

- 输出结果

```aidl


```