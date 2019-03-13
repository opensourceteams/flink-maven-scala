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




### reduce
- 对集合中所有元素，两两之间进行reduce函数表达式的计算
- 示例功能:统计所有数据的和


```aidl
package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.map

package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.reduce

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
  * 相当于进行所有元素的累加操作，求和操作
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(3,5,8,9)
    //  3 + 5 + 8 + 9


    val dataSet2 = dataSet.reduce((a,b) => {
      println(s"${a} + ${b} = ${a +b}")
      a + b
    })
    dataSet2.print()

  }

}



```
- 输出结果

```aidl
3 + 5 = 8
8 + 8 = 16
16 + 9 = 25
25


```






### reduce (先groupBy)
- 对集合中所有元素，按指定的key分组，按组执行reduce
- 示例功能:按key分组统计所有数据的和


```aidl
package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.reduce

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
  * 相当于按key进行分组,然后对组内的元素进行的累加操作，求和操作
  */
object ReduceGroupRun2 {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",1),("b",1),("c",1),("a",1),("c",1),("d",1),("f",1),("g",1),("f",1))

    /**
      * (a,1)
      * (b,1)
      * (c,1)
      * (a,1)
      * (c,1)
      * (d,1)
      * (f,1)
      * (g,1)
      */

    val dataSet2 = dataSet.groupBy(0).reduce((x,y) => {
      (x._1,x._2 + y._2)
    }
    )



    dataSet2.print()

  }

}


```
- 输出结果

```aidl
(d,1)
(a,2)
(f,2)
(b,1)
(c,2)
(g,1)

```








### groupBy   (class Fields)
- 对集合中所有元素，按用例类中的属性，进行分组
- 示例功能:按key分组统计所有数据的和


```aidl
package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.groupByClassFields

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
  * 相当于按key进行分组,然后对组内的元素进行的累加操作，求和操作
  */
object ReduceGroupRun {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements("a","b","c","a","c","d","f","g","f")

    /**
      * (a,1)
      * (b,1)
      * (c,1)
      * (a,1)
      * (c,1)
      * (d,1)
      * (f,1)
      * (g,1)
      */

    val dataSet2 = dataSet.map(WordCount(_,1)).groupBy("word").reduce((x,y) => WordCount(x.word, x.count + y.count))



    dataSet2.print()

  }

  case class WordCount(word:String,count:Int)

}



```
- 输出结果

```aidl
WordCount(d,1)
WordCount(a,2)
WordCount(f,2)
WordCount(b,1)
WordCount(c,2)
WordCount(g,1)

```



### groupBy   (key Selector)
- 对集合中所有元素，按key 选择器进行分组
- 示例功能:按key分组统计所有数据的和


```aidl
package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.groupByKeySelector

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
  * 相当于按key进行分组,然后对组内的元素进行的累加操作，求和操作
  */
object ReduceGroupRun {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements("a","b","c","a","c","d","f","g","f")

    /**
      * (a,1)
      * (b,1)
      * (c,1)
      * (a,1)
      * (c,1)
      * (d,1)
      * (f,1)
      * (g,1)
      */

    val dataSet2 = dataSet.map((_,1)).groupBy(_._1).reduce((x,y) => (x._1,x._2 +y._2))



    dataSet2.print()

  }

}


```
- 输出结果

```aidl
WordCount(d,1)
WordCount(a,2)
WordCount(f,2)
WordCount(b,1)
WordCount(c,2)
WordCount(g,1)

```




### reduceGroup
- 对集合中所有元素，按指定的key分组，把相同key的元素，做为参数，调用reduceGroup()函数
- 示例功能:按key分组统计所有数据的和


```aidl

package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.reduceGroup

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector



/**
  * 相同的key的元素，都一次做为参数传进来了
  */
object ReduceGroupRun {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataSet = env.fromElements("a","a","c","b","a")



    /**
      * 中间数据
      * (a,1)
      * (a,1)
      * (c,1)
      * (b,1)
      * (a,1)
      */
    val result = dataSet.map((_,1)).groupBy(0).reduceGroup(


      (in, out: Collector[(String,Int)]) =>{

        var count = 0 ;
        var word = "";
        while (in.hasNext){

          val next  = in.next()
          word = next._1
          count = count + next._2

        }
        out.collect((word,count))
      }


    )


    result.print()


  }

}



```
- 输出结果

```aidl
(a,3)
(b,1)
(c,1)

```









### combineGroup 
- 对集合中所有元素，按指定的key分组，把相同key的元素，做为参数，调用combineGroup()函数,会在本地进行合并
- 示例功能:按key分组统计所有数据的和


```aidl

package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.combineGroup

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector



/**
  * 相同的key的元素，都一次做为参数传进来了
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataSet = env.fromElements("a","a","c","b","a")



    /**
      * 中间数据
      * (a,1)
      * (a,1)
      * (c,1)
      * (b,1)
      * (a,1)
      */
    val result = dataSet.map((_,1)).groupBy(0).combineGroup(


      (in, out: Collector[(String,Int)]) =>{

        var count = 0 ;
        var word = "";
        while (in.hasNext){

          val next  = in.next()
          word = next._1
          count = count + next._2

        }
        out.collect((word,count))
      }


    )


    result.print()


  }

}



```
- 输出结果

```aidl
(a,3)
(b,1)
(c,1)

```



### Aggregate sum
- 按key分组 对Tuple2(String,Int) 中value进行求和操作



```aidl
package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.aggregate.sum

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
  * 相当于按key进行分组,然后对组内的元素进行的累加操作，求和操作
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",1),("g",1),("f",1))

    /**
      * (a,1)
      * (b,1)
      * (c,1)
      * (a,1)
      * (c,1)
      * (d,1)
      * (f,1)
      * (g,1)
      */

    val dataSet2 = dataSet.sum(1)



    dataSet2.print()

  }

}



```
- 输出结果

```aidl
(f,15)

```



### Aggregate max 
- 按key分组 对Tuple2(String,Int) 中value进行求最大值操作



```aidl

package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.aggregate.max

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
  * 相当于按key进行分组,然后对组内的元素进行的累加操作，求和操作
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",1),("g",1),("f",1))

    /**
      * (a,1)
      * (b,1)
      * (c,1)
      * (a,1)
      * (c,1)
      * (d,1)
      * (f,1)
      * (g,1)
      */

    val dataSet2 = dataSet.max(1)



    dataSet2.print()

  }

}




```
- 输出结果

```aidl
(f,5)

```



### Aggregate min
- 按key分组 对Tuple2(String,Int) 中value进行求最小值操作



```aidl

package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.aggregate.min

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
  * 相当于按key进行分组,然后对组内的元素进行的累加操作，求和操作
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",1),("g",1),("f",1))

    /**
      * (a,1)
      * (b,1)
      * (c,1)
      * (a,1)
      * (c,1)
      * (d,1)
      * (f,1)
      * (g,1)
      */

    val dataSet2 = dataSet.min(1)



    dataSet2.print()

  }

}




```
- 输出结果

```aidl
(f,1)

```



### Aggregate sum (groupBy)
- 按key分组 对Tuple2(String,Int) 中的所有元素进行求和操作
- 示例功能:按key分组统计所有数据的和


```aidl
package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.aggregate.sum

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
  * 相当于按key进行分组,然后对组内的元素进行的累加操作，求和操作
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",1),("b",1),("c",1),("a",1),("c",1),("d",1),("f",1),("g",1),("f",1))

    /**
      * (a,1)
      * (b,1)
      * (c,1)
      * (a,1)
      * (c,1)
      * (d,1)
      * (f,1)
      * (g,1)
      */

    val dataSet2 = dataSet.groupBy(0).sum(1)



    dataSet2.print()

  }

}



```
- 输出结果

```aidl
(d,1)
(a,2)
(f,2)
(b,1)
(c,2)
(g,1)

```






### Aggregate max (groupBy)   等于  maxBy
- 按key分组 对Tuple2(String,Int) 中value 进行求最大值
- 示例功能:按key分组统计最大值


```aidl


package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.aggregate.max

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
  * 相当于按key进行分组,然后对组内的元素进行的累加操作，求和操作
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",2),("b",1),("c",4),("a",1),("c",1),("d",1),("f",1),("g",1),("f",1))

    /**
      * (a,1)
      * (b,1)
      * (c,1)
      * (a,1)
      * (c,1)
      * (d,1)
      * (f,1)
      * (g,1)
      */

    val dataSet2 = dataSet.groupBy(0).max(1)



    dataSet2.print()

  }

}


```
- 输出结果

```aidl
(d,1)
(a,2)
(f,1)
(b,1)
(c,4)
(g,1)

```






### Aggregate min (groupBy) 等于minBy
- 按key分组 对Tuple2(String,Int) 中value 进行求最小值
- 示例功能:按key分组统计最小值


```aidl


package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.aggregate.max

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
  * 相当于按key进行分组,然后对组内的元素进行的累加操作，求和操作
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",2),("b",1),("c",4),("a",1),("c",1),("d",1),("f",1),("g",1),("f",1))

    /**
      * (a,1)
      * (b,1)
      * (c,1)
      * (a,1)
      * (c,1)
      * (d,1)
      * (f,1)
      * (g,1)
      */

    val dataSet2 = dataSet.groupBy(0).min(1)



    dataSet2.print()

  }

}


```
- 输出结果

```aidl
(d,1)
(a,1)
(f,1)
(b,1)
(c,1)
(g,1)
```





### distinct 去重
- 按指定的例，去重


```aidl
package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.aggregate.distinct

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


/**
  * 相当于按key进行分组,然后对组内的元素进行的累加操作，求和操作
  */
object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",1),("g",1),("f",1))

    /**
      * (a,1)
      * (b,1)
      * (c,1)
      * (a,1)
      * (c,1)
      * (d,1)
      * (f,1)
      * (g,1)
      */

    val dataSet2 = dataSet.distinct(1)



    dataSet2.print()

  }

}



```
- 输出结果

```aidl

(a,3)
(b,1)
(c,5)

```






### join 
- 连接


```aidl


package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.join

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",1),("g",1),("f",1))
    val dataSet2 = env.fromElements(("d",1),("f",1),("g",1),("f",1))


    //全外连接
    val dataSet3 = dataSet.join(dataSet2).where(0).equalTo(0)



    dataSet3.print()

  }

}


```
- 输出结果

```aidl

((d,1),(d,1))
((f,1),(f,1))
((f,1),(f,1))
((f,1),(f,1))
((f,1),(f,1))
((g,1),(g,1))

```




### join (Function)
- 连接


```aidl



package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.joinFunction

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",2),("g",5))
    val dataSet2 = env.fromElements(("g",1),("f",1))


    //全外连接
    val dataSet3 = dataSet.join(dataSet2).where(0).equalTo(0){
      (x,y) => (x._1,x._2+ y._2)
    }

    


    dataSet3.print()

  }

}



```
- 输出结果

```aidl

(f,3)
(g,6)

```






### leftOuterJoin 
- 左外连接,左边的Dataset中的每一个元素，去连接右边的元素


```aidl


package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.leftOuterJoin

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",2),("g",5))
    val dataSet2 = env.fromElements(("g",1),("f",1))


    //全外连接
    val dataSet3 = dataSet.leftOuterJoin(dataSet2).where(0).equalTo(0){
      (x,y) => {
        var count = 0;
        if(y != null ){
          count = y._2
        }
        (x._1,x._2+ count)
      }
    }




    dataSet3.print()

  }

}



```
- 输出结果

```aidl

(d,1)
(a,3)
(a,1)
(f,3)
(b,1)
(c,5)
(c,1)
(g,6)

```





### rightOuterJoin 
- 右外连接,左边的Dataset中的每一个元素，去连接左边的元素


```aidl

package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.rightOuterJoin

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",2),("g",5))
    val dataSet2 = env.fromElements(("g",1),("f",1))


    //全外连接
    val dataSet3 = dataSet.rightOuterJoin(dataSet2).where(0).equalTo(0){
      (x,y) => {
        var count = 0;
        if(x != null ){
          count = x._2
        }
        (x._1,y._2 + count)
      }
    }




    dataSet3.print()

  }

}



```
- 输出结果

```aidl

(f,2)
(g,2)

```




### fullOuterJoin 
- 全外连接,左右两边的元素，全部连接


```aidl

package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.fullOuterJoin

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",2),("g",5))
    val dataSet2 = env.fromElements(("g",1),("f",1))


    //全外连接
    val dataSet3 = dataSet.fullOuterJoin(dataSet2).where(0).equalTo(0){
      (x,y) => {
        var countY = 0;
        if(y != null ){
          countY = y._2
        }


        var countX = 0;
        if(x != null ){
          countX = x._2
        }
        (x._1,countX + countY)
      }
    }




    dataSet3.print()

  }

}



```
- 输出结果

```aidl

(f,2)
(g,2)

```




### union 
-  连接


```aidl

package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.union

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",1),("g",1),("f",1))
    val dataSet2 = env.fromElements(("d",1),("f",1),("g",1),("f",1))


    //全外连接
    val dataSet3 = dataSet.union(dataSet2)



    dataSet3.print()

  }

}


```
- 输出结果

```aidl

(a,1)
(d,1)
(g,1)
(f,1)
(f,1)
(g,1)
(f,1)


```




### first n 
-  前面几条数据


```aidl

package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.first

import org.apache.flink.api.scala.{ExecutionEnvironment, _}


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",3),("b",1),("c",5),("a",1),("c",1),("d",1),("f",1),("g",1),("f",1))


    //全外连接
    val dataSet3 = dataSet.first(3)



    dataSet3.print()

  }

}


```
- 输出结果

```aidl

(a,3)
(b,1)
(c,5)

```





### coGroup
-  相当于，取出两个数据集的所有去重的key,然后，再把第一个DataSet中的这个key的所有元素放到可迭代对象中，再把第二个DataSet中的这个key的所有元素放到可迭代对象中


```aidl

package com.opensourceteams.module.bigdata.flink.example.dataset.transformation.cogroup

import java.lang

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.util.Collector


object Run {

  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataSet = env.fromElements(("a",1),("g",1),("a",1))
    val dataSet2 = env.fromElements(("a",1),("f",1))


    //全外连接
    val dataSet3 = dataSet.coGroup(dataSet2).where(0).equalTo(0)
    {
      new CoGroupFunction[(String,Int),(String,Int), Collector[(String,Int)]] {
        override def coGroup(first: lang.Iterable[(String, Int)], second: lang.Iterable[(String, Int)], out: Collector[Collector[(String, Int)]]): Unit = {
          println("==============开始")
          println("first")
          println(first)
          val iteratorFirst = first.iterator()
          while (iteratorFirst.hasNext()){
            println(iteratorFirst.next())
          }

          println("second")
          println(second)
          val iteratorSecond = second.iterator()
          while (iteratorSecond.hasNext()){
            println(iteratorSecond.next())
          }
          println("==============结束")

        }
      }
    }


    dataSet3.print()

  }

}


```
- 输出结果

```aidl

/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/bin/java "-javaagent:/Applications/IntelliJ IDEA.app/Contents/lib/idea_rt.jar=54021:/Applications/IntelliJ IDEA.app/Contents/bin" -Dfile.encoding=UTF-8 -classpath /Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/charsets.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/deploy.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/ext/cldrdata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/ext/dnsns.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/ext/jaccess.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/ext/jfxrt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/ext/localedata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/ext/nashorn.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/ext/sunec.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/ext/sunjce_provider.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/ext/sunpkcs11.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/ext/zipfs.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/javaws.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/jce.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/jfr.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/jfxswt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/jsse.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/management-agent.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/plugin.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/resources.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/jre/lib/rt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/lib/ant-javafx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/lib/dt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/lib/javafx-mx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/lib/jconsole.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/lib/packager.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/lib/sa-jdi.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_191.jdk/Contents/Home/lib/tools.jar:/opt/n_001_workspaces/bigdata/flink/flink-maven-scala-2/target/classes:/Users/liuwen/.m2/repository/org/apache/flink/flink-scala_2.11/1.7.2/flink-scala_2.11-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-core/1.7.2/flink-core-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-annotations/1.7.2/flink-annotations-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-metrics-core/1.7.2/flink-metrics-core-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/commons/commons-lang3/3.3.2/commons-lang3-3.3.2.jar:/Users/liuwen/.m2/repository/com/esotericsoftware/kryo/kryo/2.24.0/kryo-2.24.0.jar:/Users/liuwen/.m2/repository/com/esotericsoftware/minlog/minlog/1.2/minlog-1.2.jar:/Users/liuwen/.m2/repository/org/objenesis/objenesis/2.1/objenesis-2.1.jar:/Users/liuwen/.m2/repository/commons-collections/commons-collections/3.2.2/commons-collections-3.2.2.jar:/Users/liuwen/.m2/repository/org/apache/commons/commons-compress/1.18/commons-compress-1.18.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-java/1.7.2/flink-java-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/commons/commons-math3/3.5/commons-math3-3.5.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-shaded-asm/5.0.4-5.0/flink-shaded-asm-5.0.4-5.0.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-shaded-asm-6/6.2.1-5.0/flink-shaded-asm-6-6.2.1-5.0.jar:/Users/liuwen/.m2/repository/org/scala-lang/scala-reflect/2.11.12/scala-reflect-2.11.12.jar:/Users/liuwen/.m2/repository/org/scala-lang/scala-compiler/2.11.12/scala-compiler-2.11.12.jar:/Users/liuwen/.m2/repository/org/scala-lang/modules/scala-xml_2.11/1.0.5/scala-xml_2.11-1.0.5.jar:/Users/liuwen/.m2/repository/org/scala-lang/modules/scala-parser-combinators_2.11/1.0.4/scala-parser-combinators_2.11-1.0.4.jar:/Users/liuwen/.m2/repository/org/slf4j/slf4j-api/1.7.15/slf4j-api-1.7.15.jar:/Users/liuwen/.m2/repository/com/google/code/findbugs/jsr305/1.3.9/jsr305-1.3.9.jar:/Users/liuwen/.m2/repository/org/apache/flink/force-shading/1.7.2/force-shading-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-streaming-scala_2.11/1.7.2/flink-streaming-scala_2.11-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-streaming-java_2.11/1.7.2/flink-streaming-java_2.11-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-runtime_2.11/1.7.2/flink-runtime_2.11-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-queryable-state-client-java_2.11/1.7.2/flink-queryable-state-client-java_2.11-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-hadoop-fs/1.7.2/flink-hadoop-fs-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-shaded-netty/4.1.24.Final-5.0/flink-shaded-netty-4.1.24.Final-5.0.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-shaded-jackson/2.7.9-5.0/flink-shaded-jackson-2.7.9-5.0.jar:/Users/liuwen/.m2/repository/org/javassist/javassist/3.19.0-GA/javassist-3.19.0-GA.jar:/Users/liuwen/.m2/repository/com/typesafe/akka/akka-actor_2.11/2.4.20/akka-actor_2.11-2.4.20.jar:/Users/liuwen/.m2/repository/com/typesafe/config/1.3.0/config-1.3.0.jar:/Users/liuwen/.m2/repository/org/scala-lang/modules/scala-java8-compat_2.11/0.7.0/scala-java8-compat_2.11-0.7.0.jar:/Users/liuwen/.m2/repository/com/typesafe/akka/akka-stream_2.11/2.4.20/akka-stream_2.11-2.4.20.jar:/Users/liuwen/.m2/repository/org/reactivestreams/reactive-streams/1.0.0/reactive-streams-1.0.0.jar:/Users/liuwen/.m2/repository/com/typesafe/ssl-config-core_2.11/0.2.1/ssl-config-core_2.11-0.2.1.jar:/Users/liuwen/.m2/repository/com/typesafe/akka/akka-protobuf_2.11/2.4.20/akka-protobuf_2.11-2.4.20.jar:/Users/liuwen/.m2/repository/com/typesafe/akka/akka-slf4j_2.11/2.4.20/akka-slf4j_2.11-2.4.20.jar:/Users/liuwen/.m2/repository/org/clapper/grizzled-slf4j_2.11/1.3.2/grizzled-slf4j_2.11-1.3.2.jar:/Users/liuwen/.m2/repository/com/github/scopt/scopt_2.11/3.5.0/scopt_2.11-3.5.0.jar:/Users/liuwen/.m2/repository/com/twitter/chill_2.11/0.7.6/chill_2.11-0.7.6.jar:/Users/liuwen/.m2/repository/com/twitter/chill-java/0.7.6/chill-java-0.7.6.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-clients_2.11/1.7.2/flink-clients_2.11-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-optimizer_2.11/1.7.2/flink-optimizer_2.11-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-shaded-guava/18.0-5.0/flink-shaded-guava-18.0-5.0.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-connector-filesystem_2.11/1.7.2/flink-connector-filesystem_2.11-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-connector-kafka_2.11/1.7.2/flink-connector-kafka_2.11-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/flink/flink-connector-kafka-base_2.11/1.7.2/flink-connector-kafka-base_2.11-1.7.2.jar:/Users/liuwen/.m2/repository/org/apache/kafka/kafka-clients/2.0.1/kafka-clients-2.0.1.jar:/Users/liuwen/.m2/repository/org/lz4/lz4-java/1.4.1/lz4-java-1.4.1.jar:/Users/liuwen/.m2/repository/org/xerial/snappy/snappy-java/1.1.7.1/snappy-java-1.1.7.1.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-client/2.9.2/hadoop-client-2.9.2.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-common/2.9.2/hadoop-common-2.9.2.jar:/Users/liuwen/.m2/repository/com/google/guava/guava/11.0.2/guava-11.0.2.jar:/Users/liuwen/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar:/Users/liuwen/.m2/repository/xmlenc/xmlenc/0.52/xmlenc-0.52.jar:/Users/liuwen/.m2/repository/org/apache/httpcomponents/httpclient/4.5.2/httpclient-4.5.2.jar:/Users/liuwen/.m2/repository/org/apache/httpcomponents/httpcore/4.4.4/httpcore-4.4.4.jar:/Users/liuwen/.m2/repository/commons-codec/commons-codec/1.4/commons-codec-1.4.jar:/Users/liuwen/.m2/repository/commons-io/commons-io/2.4/commons-io-2.4.jar:/Users/liuwen/.m2/repository/commons-net/commons-net/3.1/commons-net-3.1.jar:/Users/liuwen/.m2/repository/org/mortbay/jetty/jetty-sslengine/6.1.26/jetty-sslengine-6.1.26.jar:/Users/liuwen/.m2/repository/javax/servlet/jsp/jsp-api/2.1/jsp-api-2.1.jar:/Users/liuwen/.m2/repository/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar:/Users/liuwen/.m2/repository/commons-lang/commons-lang/2.6/commons-lang-2.6.jar:/Users/liuwen/.m2/repository/commons-configuration/commons-configuration/1.6/commons-configuration-1.6.jar:/Users/liuwen/.m2/repository/commons-digester/commons-digester/1.8/commons-digester-1.8.jar:/Users/liuwen/.m2/repository/commons-beanutils/commons-beanutils/1.7.0/commons-beanutils-1.7.0.jar:/Users/liuwen/.m2/repository/commons-beanutils/commons-beanutils-core/1.8.0/commons-beanutils-core-1.8.0.jar:/Users/liuwen/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.9.13/jackson-core-asl-1.9.13.jar:/Users/liuwen/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.9.13/jackson-mapper-asl-1.9.13.jar:/Users/liuwen/.m2/repository/org/apache/avro/avro/1.7.7/avro-1.7.7.jar:/Users/liuwen/.m2/repository/com/thoughtworks/paranamer/paranamer/2.3/paranamer-2.3.jar:/Users/liuwen/.m2/repository/com/google/protobuf/protobuf-java/2.5.0/protobuf-java-2.5.0.jar:/Users/liuwen/.m2/repository/com/google/code/gson/gson/2.2.4/gson-2.2.4.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-auth/2.9.2/hadoop-auth-2.9.2.jar:/Users/liuwen/.m2/repository/com/nimbusds/nimbus-jose-jwt/4.41.1/nimbus-jose-jwt-4.41.1.jar:/Users/liuwen/.m2/repository/com/github/stephenc/jcip/jcip-annotations/1.0-1/jcip-annotations-1.0-1.jar:/Users/liuwen/.m2/repository/net/minidev/json-smart/2.3/json-smart-2.3.jar:/Users/liuwen/.m2/repository/net/minidev/accessors-smart/1.2/accessors-smart-1.2.jar:/Users/liuwen/.m2/repository/org/ow2/asm/asm/5.0.4/asm-5.0.4.jar:/Users/liuwen/.m2/repository/org/apache/directory/server/apacheds-kerberos-codec/2.0.0-M15/apacheds-kerberos-codec-2.0.0-M15.jar:/Users/liuwen/.m2/repository/org/apache/directory/server/apacheds-i18n/2.0.0-M15/apacheds-i18n-2.0.0-M15.jar:/Users/liuwen/.m2/repository/org/apache/directory/api/api-asn1-api/1.0.0-M20/api-asn1-api-1.0.0-M20.jar:/Users/liuwen/.m2/repository/org/apache/directory/api/api-util/1.0.0-M20/api-util-1.0.0-M20.jar:/Users/liuwen/.m2/repository/org/apache/curator/curator-framework/2.7.1/curator-framework-2.7.1.jar:/Users/liuwen/.m2/repository/org/apache/curator/curator-client/2.7.1/curator-client-2.7.1.jar:/Users/liuwen/.m2/repository/org/apache/curator/curator-recipes/2.7.1/curator-recipes-2.7.1.jar:/Users/liuwen/.m2/repository/org/apache/htrace/htrace-core4/4.1.0-incubating/htrace-core4-4.1.0-incubating.jar:/Users/liuwen/.m2/repository/org/apache/zookeeper/zookeeper/3.4.6/zookeeper-3.4.6.jar:/Users/liuwen/.m2/repository/io/netty/netty/3.7.0.Final/netty-3.7.0.Final.jar:/Users/liuwen/.m2/repository/org/codehaus/woodstox/stax2-api/3.1.4/stax2-api-3.1.4.jar:/Users/liuwen/.m2/repository/com/fasterxml/woodstox/woodstox-core/5.0.3/woodstox-core-5.0.3.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-hdfs-client/2.9.2/hadoop-hdfs-client-2.9.2.jar:/Users/liuwen/.m2/repository/com/squareup/okhttp/okhttp/2.7.5/okhttp-2.7.5.jar:/Users/liuwen/.m2/repository/com/squareup/okio/okio/1.6.0/okio-1.6.0.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-mapreduce-client-app/2.9.2/hadoop-mapreduce-client-app-2.9.2.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-mapreduce-client-common/2.9.2/hadoop-mapreduce-client-common-2.9.2.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-mapreduce-client-shuffle/2.9.2/hadoop-mapreduce-client-shuffle-2.9.2.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-yarn-server-common/2.9.2/hadoop-yarn-server-common-2.9.2.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-yarn-registry/2.9.2/hadoop-yarn-registry-2.9.2.jar:/Users/liuwen/.m2/repository/org/apache/geronimo/specs/geronimo-jcache_1.0_spec/1.0-alpha-1/geronimo-jcache_1.0_spec-1.0-alpha-1.jar:/Users/liuwen/.m2/repository/org/ehcache/ehcache/3.3.1/ehcache-3.3.1.jar:/Users/liuwen/.m2/repository/com/zaxxer/HikariCP-java7/2.4.12/HikariCP-java7-2.4.12.jar:/Users/liuwen/.m2/repository/com/microsoft/sqlserver/mssql-jdbc/6.2.1.jre7/mssql-jdbc-6.2.1.jre7.jar:/Users/liuwen/.m2/repository/org/fusesource/leveldbjni/leveldbjni-all/1.8/leveldbjni-all-1.8.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-yarn-api/2.9.2/hadoop-yarn-api-2.9.2.jar:/Users/liuwen/.m2/repository/javax/xml/bind/jaxb-api/2.2.2/jaxb-api-2.2.2.jar:/Users/liuwen/.m2/repository/javax/xml/stream/stax-api/1.0-2/stax-api-1.0-2.jar:/Users/liuwen/.m2/repository/javax/activation/activation/1.1/activation-1.1.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-mapreduce-client-core/2.9.2/hadoop-mapreduce-client-core-2.9.2.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-yarn-client/2.9.2/hadoop-yarn-client-2.9.2.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-yarn-common/2.9.2/hadoop-yarn-common-2.9.2.jar:/Users/liuwen/.m2/repository/javax/servlet/servlet-api/2.5/servlet-api-2.5.jar:/Users/liuwen/.m2/repository/org/mortbay/jetty/jetty-util/6.1.26/jetty-util-6.1.26.jar:/Users/liuwen/.m2/repository/com/sun/jersey/jersey-core/1.9/jersey-core-1.9.jar:/Users/liuwen/.m2/repository/com/sun/jersey/jersey-client/1.9/jersey-client-1.9.jar:/Users/liuwen/.m2/repository/org/codehaus/jackson/jackson-jaxrs/1.9.13/jackson-jaxrs-1.9.13.jar:/Users/liuwen/.m2/repository/org/codehaus/jackson/jackson-xc/1.9.13/jackson-xc-1.9.13.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-mapreduce-client-jobclient/2.9.2/hadoop-mapreduce-client-jobclient-2.9.2.jar:/Users/liuwen/.m2/repository/org/apache/hadoop/hadoop-annotations/2.9.2/hadoop-annotations-2.9.2.jar:/Users/liuwen/.m2/repository/org/scala-lang/scala-library/2.11.12/scala-library-2.11.12.jar:/Users/liuwen/.m2/repository/com/alibaba/fastjson/1.2.56/fastjson-1.2.56.jar:/Users/liuwen/.m2/repository/org/slf4j/slf4j-log4j12/1.7.7/slf4j-log4j12-1.7.7.jar:/Users/liuwen/.m2/repository/log4j/log4j/1.2.17/log4j-1.2.17.jar com.opensourceteams.module.bigdata.flink.example.dataset.transformation.cogroup.Run
==============开始
first
org.apache.flink.runtime.util.NonReusingKeyGroupedIterator$ValuesIterator@3500e7b0
(a,1)
(a,1)
second
org.apache.flink.runtime.util.NonReusingKeyGroupedIterator$ValuesIterator@41230ea2
(a,1)
==============结束
==============开始
first
org.apache.flink.runtime.util.NonReusingKeyGroupedIterator$ValuesIterator@14602d0a
(g,1)
second
[]
==============结束
==============开始
first
[]
second
org.apache.flink.runtime.util.NonReusingKeyGroupedIterator$ValuesIterator@2b0a15b5
(f,1)
==============结束

Process finished with exit code 0


```