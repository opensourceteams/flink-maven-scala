# Flink1.7.2 sql 示例


## DML

### insert into 
- 功能描述，将一个表中的数据(source)，插入到 csv文件中(sink)
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
