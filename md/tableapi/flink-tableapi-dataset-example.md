# flink1.7.2 tableapi批处理示例

### print table 
- 功能描述: 打印输出表数据
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.convert.dataset

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run2 {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    table.first(1000).print()


    /**
      * 打印输出表数据
      *
      * 1,a,10
      * 2,b,20
      * 3,c,30
      */


  }

}


```

- 输出结果

```aidl
1,a,10
2,b,20
3,c,30

```

### DataSet 转换成table

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.convert.dataset

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run1 {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    //注册table
    tableEnv.registerTable("user1",table)


    //查询table 所有数据
    tableEnv.scan("user1").first(10)

      //print 输出 (相当于sink)
      .print()


    /**
      * 输出结果
      * 
      * 1,a,10
      * 2,b,20
      * 3,c,30
      */



  }

}

```

- 输出结果

```aidl
1,a,10
2,b,20
3,c,30

```




### Scan 
- 功能描述: 查询表中所有数据
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.scan

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    //注册table
    tableEnv.registerTable("user1",table)


    //查询table 所有数据
    tableEnv.scan("user1").first(100)

      //print 输出 (相当于sink)
      .print()


    /**
      * 输出结果
      *
      * 1,a,10
      * 2,b,20
      * 3,c,30
      */



  }

}

```

- 输出结果

```aidl

1,a,10
2,b,20
3,c,30
```


### select 
- 功能描述: 选择表中需要的字段
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.select

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    //注册table
    tableEnv.registerTable("user1",table)


    //查询table 所有数据
    tableEnv.scan("user1")
      //选择需要的字段
      .select('_1,'_2,'_3)
      .first(100)

      //print 输出 (相当于sink)
      .print()


    /**
      * 输出结果
      *
      * 1,a,10
      * 2,b,20
      * 3,c,30
      */



  }

}


```

- 输出结果

```aidl
1,a,10
2,b,20
3,c,30

```




### as 
- 功能描述:  重命名字段名称
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.as

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    //注册table
    tableEnv.registerTable("user1",table)


    //查询table 所有数据
    tableEnv.scan("user1")

      //重命令字段名称
      .as('id,'name,'value)
      //选择需要的字段
      .select('id,'name,'value)
      .first(100)

      //print 输出 (相当于sink)
      .print()


    /**
      * 输出结果
      *
      * 1,a,10
      * 2,b,20
      * 3,c,30
      */



  }

}


```

- 输出结果

```aidl
1,a,10
2,b,20
3,c,30

```




### as 
- 功能描述:  重命名字段名称
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.as

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    //注册table
    tableEnv.registerTable("user1",table)


    //查询table 所有数据
    tableEnv.scan("user1")

      //重命令字段名称
      .as('id,'name,'value)
      //选择需要的字段
       .select('id,'name as 'name2,'value)
      .first(100)

      //print 输出 (相当于sink)
      .print()


    /**
      * 输出结果
      *
      * 1,a,10
      * 2,b,20
      * 3,c,30
      */



  }

}


```

- 输出结果

```aidl
1,a,10
2,b,20
3,c,30

```


### where / filter  (过滤字段,字符串)
- 功能描述: 条件过滤
- scala 程序

```aidl
package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.where

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30), (4,"c",20) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    //注册table
    tableEnv.registerTable("user1",table)


    //查询table 所有数据
    tableEnv.scan("user1")

      //重命令字段名称
      .as('id,'name,'value)
      //选择需要的字段
      .select('id,'name,'value)
      //条件过滤
      .where("value=20")
      .where("id=4")

      .first(100)

      //print 输出 (相当于sink)
      .print()


    /**
      * 输出结果
      *
      * 1,a,10
      * 2,b,20
      * 3,c,30
      */



  }

}


```

- 输出结果

```aidl
4,c,20

```



### where / filter  (过滤字段,表达式)  
- 功能描述: 过滤数据
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.where

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run2 {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30), (4,"c",20) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    //注册table
    tableEnv.registerTable("user1",table)


    //查询table 所有数据
    tableEnv.scan("user1")

      //重命令字段名称
      .as('id,'name,'value)
      //选择需要的字段
      .select('id,'name,'value)
      //条件过滤
      .where('value === 20)
      .where('id === 4)


      .first(100)

      //print 输出 (相当于sink)
      .print()


    /**
      * 输出结果
      *
      * 1,a,10
      * 2,b,20
      * 3,c,30
      */



  }

}


```

- 输出结果

```aidl
4,c,20

```



### groupBy 
- 功能描述: 分组统计
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.groupBy

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30), (4,"c",40) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    //注册table
    tableEnv.registerTable("user1",table)


    //查询table 所有数据
    tableEnv.scan("user1")

      //重命令字段名称
      .as('id,'name,'value)

      //选择需要的字段


      .groupBy('name)

      .select('name,'value.sum as 'value)





      .first(100)

      //print 输出 (相当于sink)
      .print()


    /**
      * 输出结果
      *
      * 1,a,10
      * 2,b,20
      * 3,c,30
      */



  }

}


```

- 输出结果
- 70 = 30 + 40

```aidl
a,10
b,20
c,70

```



### distinct 
- 功能描述:  查询记录去重
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.distinct

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(1,"a",10),(2,"b",20), (3,"c",30) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    //注册table
    tableEnv.registerTable("user1",table)


    //查询table 所有数据
    tableEnv.scan("user1")
      //记录去重
      .distinct()


      .first(100)

      //print 输出 (相当于sink)
      .print()


    /**
      * 输出结果
      *
      * 1,a,10
      * 2,b,20
      * 3,c,30
      */



  }

}


```

- 输出结果

```aidl

1,a,10
3,c,30
2,b,20
```




### distinct 
- 功能描述:  sum.distinct ,去掉字段重复的再求和
- scala 程序

```aidl
package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.distinct

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run2 {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(1,"a",10),(2,"b",20), (3,"c",30),(20,"b",20) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    //注册table
    tableEnv.registerTable("user1",table)


    //查询table 所有数据
    tableEnv.scan("user1")
      //去掉字段重复的再求和
      .select('_3.sum.distinct)


      .first(100)

      //print 输出 (相当于sink)
      .print()


    /**
      * 输出结果
      * 60
      */



  }

}


```

- 输出结果

```aidl
60

```



### join 
- 功能描述: 内连接

- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.innerJoin

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )
    val dataSet2 = env.fromElements( (1,"a",100),(20,"b",20), (30,"c",30) )



    //列不能重复
    val table = tableEnv.fromDataSet(dataSet,'a,'b,'c)
    val table2 = tableEnv.fromDataSet(dataSet2,'d,'e,'f)



   table.join(table2).where(" a = d ").first(1000).print()






  }

}


```

- 输出结果

```aidl

1,a,10,1,a,100

```






### leftOuterJoin   
- 功能描述:  左外连接，用左表中的每一个元素，去连接右表中的元素，如果右表中存在，就匹配值，如呆不存在就为空值
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.leftOuterJoin

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )
    val dataSet2 = env.fromElements( (1,"a",100),(20,"b",20), (30,"c",30) )



    //列不能重复
    val table = tableEnv.fromDataSet(dataSet,'a,'b,'c)
    val table2 = tableEnv.fromDataSet(dataSet2,'d,'e,'f)



   //table.leftOuterJoin(table2,"a=d").first(1000).print()
   table.leftOuterJoin(table2,'a === 'd).first(1000).print()


    /**
      * 输出结果
      *
      * 2,b,20,null,null,null
      * 1,a,10,1,a,100
      * 3,c,30,null,null,null
      */



  }

}


```

- 输出结果

```aidl
1,a,10,1,a,100
2,b,20,null,null,null
3,c,30,null,null,null

```



### rightOuterJoin 
- 功能描述: 右外连接，用右表中的每一个元素，去连接左表中的元素，如果左表中存在，就匹配值，如呆不存在就为空值
- scala 程序

```aidl
package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.rightOuterJoin

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )
    val dataSet2 = env.fromElements( (1,"a",100),(20,"b",20), (30,"c",30) )



    //列不能重复
    val table = tableEnv.fromDataSet(dataSet,'a,'b,'c)
    val table2 = tableEnv.fromDataSet(dataSet2,'d,'e,'f)



   table.rightOuterJoin(table2,"a = d").first(1000).print()


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


```

- 输出结果

```aidl
null,null,null,20,b
null,null,null,30,c
1,a,10,1,a,100

```





### union 
- 功能描述: 两个表串连，取并集(会去重)
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.union

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )
    val dataSet2 = env.fromElements( (1,"a",100),(2,"b",20),(20,"b",20), (30,"c",30) )



    //列不能重复
    val table = tableEnv.fromDataSet(dataSet,'a,'b,'c)
    val table2 = tableEnv.fromDataSet(dataSet2,'d,'e,'f)



   table.union(table2).first(1000).print()

    /**
      * 输出结果
      *
      * 30,c,30
      * 1,a,100
      * 2,b,20
      * 20,b,20
      * 1,a,10
      * 3,c,30
      */






  }

}


```

- 输出结果

```aidl
30,c,30
1,a,100
2,b,20
20,b,20
1,a,10
3,c,30

```





### unionAll  两个表串连，取并集(不会去重)
- 功能描述:
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.unionAll

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )
    val dataSet2 = env.fromElements( (1,"a",100),(2,"b",20),(20,"b",20), (30,"c",30) )



    //列不能重复
    val table = tableEnv.fromDataSet(dataSet,'a,'b,'c)
    val table2 = tableEnv.fromDataSet(dataSet2,'d,'e,'f)



   table.unionAll(table2).first(1000).print()

    /**
      * 输出结果
      *
      * 30,c,30
      * 1,a,100
      * 2,b,20
      * 20,b,20
      * 1,a,10
      * 3,c,30
      */






  }

}


```

- 输出结果

```aidl
1,a,10
2,b,20
3,c,30
1,a,100
2,b,20
20,b,20
30,c,30

```





### intersect,两个表相连接，取交集 (会去重)
- 功能描述:
- scala 程序

```aidl


package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.intersect

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )
    val dataSet2 = env.fromElements( (1,"a",100),(2,"b",20),(20,"b",20), (30,"c",30) )



    //列不能重复
    val table = tableEnv.fromDataSet(dataSet,'a,'b,'c)
    val table2 = tableEnv.fromDataSet(dataSet2,'d,'e,'f)



   table.intersect(table2).first(1000).print()

    /**
      * 输出结果
      *
      * 2,b,20
      */






  }

}


```

- 输出结果

```aidl
2,b,20

```





### intersectAll,两个表相连接，取交集 (不会去重)
- 功能描述:
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.intersectAll

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)


    val dataSet = env.fromElements( (1,"a",10),(2,"b",20),(2,"b",20),(2,"b",20), (3,"c",30) )
    val dataSet2 = env.fromElements( (1,"a",100),(2,"b",20),(2,"b",20),(20,"b",20), (30,"c",30) )



    //列不能重复
    val table = tableEnv.fromDataSet(dataSet,'a,'b,'c)
    val table2 = tableEnv.fromDataSet(dataSet2,'d,'e,'f)



   table.intersectAll(table2).first(1000).print()

    /**
      * 输出结果
      *
      * 2,b,20
      */






  }

}



```

- 输出结果

```aidl
2,b,20
2,b,20
```





### minus 
- 功能描述: 左表不存在于右表中的数据，会去重
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.minus

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)


    val dataSet = env.fromElements( (1,"a",10),(2,"b",20),(2,"b",20),(2,"b",20), (3,"c",30) )
    val dataSet2 = env.fromElements( (1,"a",100),(2,"b",20),(2,"b",20),(20,"b",20), (30,"c",30) )




    val table = tableEnv.fromDataSet(dataSet,'a,'b,'c)
    val table2 = tableEnv.fromDataSet(dataSet2,'d,'e,'f)


    /**
      * 左表不存在于右表中的数据，会去重
      */
   table.minus(table2).first(1000).print()

    /**
      * 输出结果
      * 1,a,10
      * 3,c,30
      */






  }

}


```

- 输出结果

```aidl
1,a,10
3,c,30

```



### minusAll 
- 功能描述: 左表不存在于右表中的数据，不会去重，如果左表某个元素有n次，右表中有m次，那这个元素出现的是n - m次
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.minusAll

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)


    val dataSet = env.fromElements( (1,"a",10),(2,"b",20),(2,"b",20),(2,"b",20),(2,"b",20), (3,"c",30) )
    val dataSet2 = env.fromElements( (1,"a",100),(2,"b",20),(2,"b",20),(20,"b",20), (30,"c",30) )




    val table = tableEnv.fromDataSet(dataSet,'a,'b,'c)
    val table2 = tableEnv.fromDataSet(dataSet2,'d,'e,'f)


    /**
      * 左表不存在于右表中的数据，不会去重，如果左表某个元素有n次，右表中有m次，那这个元素出现的是n - m次
      */
   table.minusAll(table2).first(1000).print()

    /**
      * 输出结果
      *
      * 1,a,10
      * 2,b,20
      * 2,b,20
      * 3,c,30
      */






  }

}


```

- 输出结果

```aidl
1,a,10
2,b,20
2,b,20
3,c,30

```





### in 
- 功能描述:表和子表的关系,子查询只能由一列组成，
   表的查询条件的列类型需要和子查询保持一致,
   如果子查询中的值在表中存在就返回真，这个元素就满足条件可以被返回来
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.in

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20), (3,"c",30) )
    val dataSet2 = env.fromElements( (1,"a",100),(20,"b",20), (30,"c",30) )



    //列不能重复
    val table = tableEnv.fromDataSet(dataSet,'a,'b,'c)
    val table2 = tableEnv.fromDataSet(dataSet2,'d)


    /**
      * 表和子表的关系
      * 子查询只能由一列组成，表的查询条件的列类型需要和子查询保持一致
      * 如果子查询中的值在表中存在就返回真，这个元素就满足条件可以被返回来
      */
   table.where('a.in(table2))

      .first(1000).print()


    /**
      * 输出结果
      *
      * 1,a,10
      */



  }

}


```

- 输出结果

```aidl

1,a,10
```







### orderBy  
- 功能描述: 按指定列的升序或降序排序(是按分区来排序的)
- 经测试只能按一列进行排骗子
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.orderBy

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    env.setParallelism(1)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20) ,(20,"f",200),(3,"c",30) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    //注册table
    tableEnv.registerTable("user1",table)


    //查询table 所有数据
    tableEnv.scan("user1").as('id,'name,'value1)
      //.orderBy('id.asc)  //按id列，升序排序(注意是按分区来排序)
      .orderBy('id.desc)
      //.orderBy('value1.asc)

      .first(1000)

      //print 输出 (相当于sink)
      .print()


    /**
      * 输出结果
      * 
      * 20,f,200
      * 3,c,30
      * 2,b,20
      * 1,a,10
      */



  }

}


```

- 输出结果

```aidl
20,f,200
3,c,30
2,b,20
1,a,10

```



### fetch 
- 功能描述: 先进行排序后，取前几个元素
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.fetch

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    env.setParallelism(1)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20) ,(20,"f",200),(3,"c",30) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    //注册table
    tableEnv.registerTable("user1",table)


    //查询table 所有数据
    tableEnv.scan("user1").as('id,'name,'value1)
      //.orderBy('id.asc)  //按id列，升序排序(注意是按分区来排序)
       .orderBy('id.desc)

      .fetch(2)  //只有有序的才能用，只取了2个元素

      .first(1000)
      //print 输出 (相当于sink)
      .print()


    /**
      * 输出结果
      *
      * 20,f,200
      * 3,c,30
      */



  }

}


```

- 输出结果

```aidl
20,f,200
3,c,30

```




### offset 
- 功能描述: 只有有序的才能用，偏移了2个元素
- scala 程序

```aidl

package com.opensourceteams.module.bigdata.flink.example.tableapi.operation.offset

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {


  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    env.setParallelism(1)

    val dataSet = env.fromElements( (1,"a",10),(2,"b",20) ,(20,"f",200),(3,"c",30) )



    //从dataset转化为 table
    val table = tableEnv.fromDataSet(dataSet)

    //注册table
    tableEnv.registerTable("user1",table)


    //查询table 所有数据
    tableEnv.scan("user1").as('id,'name,'value1)
      //.orderBy('id.asc)  //按id列，升序排序(注意是按分区来排序)
       .orderBy('id.desc)

      .offset(2)  //只有有序的才能用，偏移了2个元素

      .first(1000)
      //print 输出 (相当于sink)
      .print()


    /**
      * 输出结果
      *
      * 2,b,20
      * 1,a,10
      */



  }

}


```

- 输出结果

```aidl
2,b,20
1,a,10

```






