# Flink 1.7.2 dataset transformation

## 源码
- https://github.com/opensourceteams/flink-maven-scala

## transformation


### map
- 对集合元素，进行一一遍历处理
- 示例功能:给集合中的每一一行，都拼接字符串


```aidl
package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.map

import org.apache.flink.api.scala.ExecutionEnvironment

import org.apache.flink.api.scala._

object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements("c a b d a c","d c a b c d")


    val dataSet2 = dataSet.map(_.toUpperCase + "字符串连接")
    dataSet2.print()

  }

}



```
- 输出结果

```aidl
C A B D A C字符串连接
D C A B C D字符串连接

```



### flatMap
- 对集合元素，进行一一遍历处理,并把子集合中的数据拉到一个集合中
- 示例功能:把行进行拆分后，再把不同的行拆分之后的元素，汇总到一个集合中


```aidl

package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.flatmap

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements("c a b d a c","d c a b c d")


    val dataSet2 = dataSet.flatMap(_.toUpperCase().split(" "))
    dataSet2.print()

  }

}



```
- 输出结果

```aidl
C
A
B
D
A
C
D
C
A
B
C
D

```




### filter
- 对集合元素，进行一一遍历处理,只过滤满足条件的元素
- 示例功能:过滤空格数据


```aidl
package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.filter

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  * filter 过滤器，对数据进行过滤处理
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements("c a b d a    c","d c   a b c d")


    val dataSet2 = dataSet.flatMap(_.toUpperCase().split(" ")).filter(_.nonEmpty)
    dataSet2.print()

  }

}


```
- 输出结果

```aidl
C
A
B
D
A
C
D
C
A
B
C
D


```