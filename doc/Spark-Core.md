













# WordCount

## 实现一

```scala
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // 1 建立和Spark的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sparkContext = new SparkContext(sparkConf)
    // 2 执行业务
    //
    val lines: RDD[String] = sparkContext.textFile("datas")
    var words: RDD[String] = lines.flatMap(_.split(" "))
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount=wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    val array: Array[(String, Int)] = wordCount.collect()
    array.foreach(println)

    // 3 关闭连接
    sparkContext.stop()

  }
}
```

## 实现二

```scala
package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount1 {

    def main(args: Array[String]): Unit = {

        // Application
        // Spark框架
        // TODO 建立和Spark框架的连接
        // JDBC : Connection
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        // TODO 执行业务操作

        // 1. 读取文件，获取一行一行的数据
        //    hello world
        val lines: RDD[String] = sc.textFile("datas")

        // 2. 将一行数据进行拆分，形成一个一个的单词（分词）
        //    扁平化：将整体拆分成个体的操作
        //   "hello world" => hello, world, hello, world
        val words: RDD[String] = lines.flatMap(_.split(" "))

        // 3. 将单词进行结构的转换,方便统计
        // word => (word, 1)
        val wordToOne = words.map(word=>(word,1))

        // 4. 将转换后的数据进行分组聚合
        // 相同key的value进行聚合操作
        // (word, 1) => (word, sum)
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

        // 5. 将转换结果采集到控制台打印出来
        val array: Array[(String, Int)] = wordToSum.collect()
        array.foreach(println)

        // TODO 关闭连接
        sc.stop()

    }
}

```

## 实现三

```scala
package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_WordCount {
    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        wordcount91011(sc)

        sc.stop()

    }

    // groupBy
    def wordcount1(sc : SparkContext): Unit = {

        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val group: RDD[(String, Iterable[String])] = words.groupBy(word=>word)
        val wordCount: RDD[(String, Int)] = group.mapValues(iter=>iter.size)
    }

    // groupByKey
    def wordcount2(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
        val wordCount: RDD[(String, Int)] = group.mapValues(iter=>iter.size)
    }

    // reduceByKey
    def wordcount3(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_+_)
    }

    // aggregateByKey
    def wordcount4(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_+_, _+_)
    }

    // foldByKey
    def wordcount5(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_+_)
    }

    // combineByKey
    def wordcount6(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
            v=>v,
            (x:Int, y) => x + y,
            (x:Int, y:Int) => x + y
        )
    }

    // countByKey
    def wordcount7(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: collection.Map[String, Long] = wordOne.countByKey()
    }

    // countByValue
    def wordcount8(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordCount: collection.Map[String, Long] = words.countByValue()
    }

    // reduce, aggregate, fold
    def wordcount91011(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))

        // 【（word, count）,(word, count)】
        // word => Map[(word,1)]
        val mapWord = words.map(
            word => {
                mutable.Map[String, Long]((word,1))
            }
        )

       val wordCount = mapWord.reduce(
            (map1, map2) => {
                map2.foreach{
                    case (word, count) => {
                        val newCount = map1.getOrElse(word, 0L) + count
                        map1.update(word, newCount)
                    }
                }
                map1
            }
        )

        println(wordCount)
    }

}

```

# Spark核心编程

Spark计算框架为了能够进行高并发和高吞吐的数据处理，封装了三大数据结构，用于处理不同的应用场景。三大数据结构分别是：
➢ RDD : 弹性分布式数据集
➢ 累加器：分布式**共享只写**变量
➢ 广播变量：分布式**共享只读**变量
接下来我们一起看看这三大数据结构是如何在数据处理中使用的。

# 一、RDD

## 1 Rdd 基本概念

 - RDD是最小的计算单元
 - 首选位置：数据发送到哪个节点效率最优 
 - 移动数据不如移动计算

## 2 RDD的并行度与分区

1. 读取内存数据

```scala
makeRDD方法可以传递第二个参数，这个参数表示分区的数量
第二个参数可以不传递的，那么makeRDD方法会使用默认值 ： defaultParallelism（默认并行度
默认的并行度：scheduler.conf.getInt("spark.default.parallelism", totalCores)
spark在默认情况下，从配置对象中获取配置参数：spark.default.parallelism
如果获取不到，那么使用totalCores属性，这个属性取值为当前运行环境的最大可用核数
```

数据如何存放到哪个分区：核心源码

```scala
// Sequences need to be sliced at the same set of index positions for operations
// like RDD.zip() to behave as expected
def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
  (0 until numSlices).iterator.map { i =>
    val start = ((i * length) / numSlices).toInt
    val end = (((i + 1) * length) / numSlices).toInt
    (start, end)
  }
}
```

2. 读取文件系统

   ```scala
   // textFile是按照行读取，读取的是一个字符串
   // wholeTextFiles是按照文件读取，读取的元组第一个是文件的路径，第二个是文件的内容
   val fileRdd: RDD[(String, String)] = sc.wholeTextFiles("datas/*")
   ```

文件系统如何分区？

- 文件系统分区数的计算
  1. 计算每个分区文件的大小 ：goalSize=totalSize/numSplits
     - 文件总的字节大小除以分区数，如果未设定分区数，则取1，如果设定分区数，则取设定的值。
  2. 计算分区数：realnumSplits=totalSize/goalSize
     - 总的文件大小除以每个分区的文件大小
     - 如果数据源为多个文件，那么计算分区时以文件为单位进行分区

```scala
object Spark02_Rdd_File_Par {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc: SparkContext = new SparkContext(sparkConf)

    // textFile可以将文件作为数据处理的数据源，默认也可以设定分区。
    //     minPartitions : 最小分区数量
    //     math.min(defaultParallelism, 2)
    //val rdd = sc.textFile("datas/1.txt")
      
    // 如果不想使用默认的分区数量，可以通过第二个参数指定分区数
    // Spark读取文件，底层其实使用的就是Hadoop的读取方式
    // 分区数量的计算方式：总文件的大小除以每个分区文件的大小，每个分区文件的大小等于文件总大小除以分区数，如果不设定分区数，默认是1。
      //  （源码）long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
      
    //    totalSize = 7 文件总字节大小
    //    goalSize =  7 / 2 = 3（byte）每个分区3字节大小
    //    hadoop读取文件的时候，有个1.1原则，如果剩余的文件大小大于10%，则创建一个新的分区，否则将剩余的数据放到前面的分区中
    //    7 / 3 = 2...1 (1.1) + 1 = 3(分区) 总文件的大小除以每个分区文件的大小为分区数

    val fileRdd: RDD[String] = sc.textFile("datas/6.txt",2)

    fileRdd.saveAsTextFile("output")
    sc.stop()
  }
}
```

文件系统数据如何存放到哪个分区：

```scala
object Spark02_Rdd_File_Par1 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO 数据分区的分配
    // 1. 数据以行为单位进行读取
    //    spark读取文件，采用的是hadoop的方式读取，所以 一行一行 读取，和字节数没有关系
    // 2. 数据读取时以 偏移量 为单位,偏移量不会被重复读取
    /*
       1@@   => 012
       2@@   => 345
       3     => 6
     */
    // 3. 数据分区的偏移量范围的计算
    // 0 => [0, 3]  => 12
    // 1 => [3, 6]  => 3
    // 2 => [6, 7]  =>

    // 【1,2】，【3】，【】
    val fileRdd: RDD[String] = sc.textFile("datas/6.txt",2)
    fileRdd.saveAsTextFile("output")
    sc.stop()
  }
}
```

分区数据的分配：案例分析

```scala
object Spark02_Rdd_File_Par2 {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD

    // 14byte / 2 = 7byte
    // 14 / 7 = 2(分区)

    /*
    1234567@@  => 012345678
    89@@       => 9101112
    0          => 13

    [0, 7]   => 1234567
    [7, 14]  => 890

     */

    // 如果数据源为多个文件，那么计算分区时以文件为单位进行分区
    val rdd = sc.textFile("datas/word.txt", 2)

    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()
  }
}
```

## 3 RDD算子

RDD方法分为两类：

- 转换：功能的补充和封装，将旧的RDD保存为新的RDD
  - map、flatmap
- 行动：触发任务的调度和作业的执行
  - collect

## RDD 转换算子

### 3.1 map

1. rdd的计算一个分区内的数据是一个一个执行逻辑

   只有前面一个数据全部的逻辑执行完毕后，才会执行下一个数据。

   分区内数据的执行是有序的。

2. 不同分区数据计算是无序的。

### 3.2 mapPartitions

mapPartitions : 可以以分区为单位进行数据转换操作

- 但是会将整个分区的数据加载到内存进行引用

- 如果处理完的数据是不会被释放掉，存在对象的引用。

- 在内存较小，数据量较大的场合下，容易出现内存溢出。

### 3.3 mapPartitionsWithIndex

将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处理，哪怕是过滤数据，在处理时同时可以获取当前分区索引。

### 3.4 flatMap

将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射

### 3.5 glom

将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变

### 3.6 groupBy

groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
相同的key值的数据会放置在一个组中

将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样的操作称之为shuffle。极限情况下，数据可能被分在同一个分区中
一个组的数据在一个分区中，但是并不是说一个分区中只有一个组

### 3.7 filter

将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出现数据倾斜。

### 3.8 sample

根据指定的规则从数据集中抽取数据，可以用于 **处理数据倾斜**

 ### 3.9 distinct

将数据集中重复的数据去重

### 3.10 coalesce

**根据数据量缩减分区**，用于大数据集过滤后，提高小数据集的执行效率。
当spark程序中，存在过多的小任务的时候，可以通过coalesce方法，收缩合并分区，减少分区的个数，减小任务调度成本。

- coalesce方法默认情况下不会将分区的数据打乱重新组合
- 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
- 如果想要让数据均衡，可以进行**shuffle处理**

coalesce算子可以扩大分区的，但是如果不进行shuffle操作，是没有意义，不起作用。

所以如果想要实现扩大分区的效果，需要使用shuffle操作。

spark提供了一个简化的操作：

- 缩减分区：coalesce，如果想要数据均衡，可以采用shuffle
- 扩大分区：repartition, 底层代码调用的就是coalesce，而且肯定采用shuffle

### 3.11 repartition

该操作内部其实执行的是coalesce操作，参数shuffle的默认值为true。

无论是将分区数多的RDD转换为分区数少的RDD，还是将分区数少的RDD转换为分区数多的RDD，repartition操作都可以完成，因为无论如何都会经shuffle过程。

### 3.12 sortBy

 sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式。
 sortBy默认情况下，不会改变分区。但是中间存在shuffle操作。

### 3.13 双Value类型

- intersection：交集
- union：并集
- subtract：差集
- zip：拉链

交集，并集和差集要求两个数据源数据类型保持一致。
拉链操作两个数据源的类型可以不一致。

拉链

- 两个数据源要求分区数量要保持一致

  - ```
    Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    ```

- 两个数据源要求分区中数据数量保持一致

  - ```
    Can only zip RDDs with same number of elements in each partition
    ```



### 3.14 key-value 类型

#### 1 partitionBy

将数据按照指定Partitioner重新进行分区。Spark默认的分区器是HashPartitioner

- 思考一个问题：如果重分区的分区器和当前RDD的分区器一样怎么办？
  - 那就无法实现分区
- 思考一个问题：Spark还有其他分区器吗？
  - 一共有三个分区器
    - HashPartitioner：默认
    - RangePartitioner：一般用于排序
    - PythonPartitioner：privite的，无法从外部使用
- 思考一个问题：如果想按照自己的方法进行数据分区怎么办？
- 思考一个问题：哪那么多问题？

#### 2 reduceByKey

 可以将数据按照相同的Key对Value进行聚合

```scala
object Spark14_RDD_Operator_Transform_reduceByKey {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3),("b",3)),2)
        // reduceByKey : 相同的key的数据进行value数据的聚合操作
        // scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
        // 【1，2，3】
        // 【3，3】
        // 【6】
        // reduceByKey中如果key的数据只有一个，是不会参与运算的。
        val resRdd: RDD[(String, Int)] = rdd.reduceByKey(_+_)

        resRdd.collect().foreach(println)

        sc.stop()
    }
}
```

#### 3 groupByKey

将数据源的数据根据key对value进行分组

- 将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
- 元组中的第一个元素就是key
- 元组中的第二个元素就是相同key的value的集合

**思考一个问题：reduceByKey和groupByKey的区别？**

scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合。

spark中，shullfe操作必须落盘处理，不能中在内存中数据等待，否则会出现内存溢出，shullfe操作的性能非常低。

- **从shuffle的角度**：reduceByKey和groupByKey都存在shuffle的操作，但是reduceByKey可以在shuffle前对分区内相同key的数据进行预聚合（combine）功能，这样会减少落盘的数据量，而groupByKey只是进行分组，不存在数据量减少的问题，reduceByKey性能比较高。
- **从功能的角度**：reduceByKey其实包含分组和聚合的功能。GroupByKey只能分组，不能聚合，所以在分组聚合的场合下，推荐使用reduceByKey，如果仅仅是分组而不需要聚合。那么还是只能使用groupByKey

**reduceByKey**

- 支持分区内聚合功能，可以有效的减少sheffle时落盘的数量，提升性能。
- **分区内和分区间计算规则是 相同的**

#### 4 aggregateByKey

将数据根据**不同的规则**进行分区内计算和分区间计算

aggregateByKey存在函数柯里化，有两个参数列表

- 第一个参数列表,需要传递一个参数，表示为初始值
  - 主要用于当碰见第一个key的时候，和value进行==分区内==计算

- 第二个参数列表需要传递2个参数
  - 第一个参数表示分区内计算规则
  - 第二个参数表示分区间计算规则

aggregateByKey最终的返回数据结果应该和初始值的类型保持一致

小练习：获取相同key的数据的平均值 => (a, 3),(b, 4)

```scala
object Spark14_RDD_Operator_Transform_aggregateByKey_Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    /**
      * 获取相同key的数据的平均值 => (a, 3),(b, 4)
      */
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    // aggregateByKey最终的返回数据结果应该和初始值的类型保持一致

    // 获取相同key的数据的平均值 => (a, 3),(b, 4)

    //(0,0)第一个0表示数量，第二个0表示次数
    val aggRdd: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => (t._1 + v, t._2 + 1), (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)
    )
    val resRdd: RDD[(String, Int)] = aggRdd.mapValues {
      case (v1, v2) => {
        v1 / v2
      }
    }

    resRdd.collect().foreach(println)

    sc.stop()
  }
}
```

#### 5 foldByKey

当分区内计算规则和分区间计算规则相同时，aggregateByKey就可以简化为foldByKey

```scala
object Spark14_RDD_Operator_Transform_foldByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)), 2)

    // 如果聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法
    val resRdd: RDD[(String, Int)] = rdd.foldByKey(0)( _ + _)

    resRdd.collect().foreach(println)

    sc.stop()

  }
}
```

#### 6 combineByKey

最通用的对key-value型rdd进行聚集操作的聚集函数（aggregation function）。类似于aggregate()，combineByKey()允许用户返回值的类型与输入不一致。

```scala
object Spark14_RDD_Operator_Transform_combineByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ),2)

    // combineByKey : 方法需要三个参数
    // 第一个参数表示：将相同key的第一个数据进行结构的转换，实现操作
    // 第二个参数表示：分区内的计算规则
    // 第三个参数表示：分区间的计算规则

    val combinRdd: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => (t._1 + v, t._2 + 1),
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2))

    val resRdd: RDD[(String, Int)] = combinRdd.mapValues {
      case (k, v) => {
        k / v
      }
    }
    resRdd.collect().foreach(println)
    sc.stop()
  }
}
```

总结：

思考一个问题：reduceByKey、foldByKey、aggregateByKey、combineByKey的区别？

- reduceByKey: 相同key的第一个数据不进行任何计算，分区内和分区间计算规则相同
- FoldByKey: 相同key的第一个数据和初始值进行分区内计算，分区内和分区间计算规则相同
- AggregateByKey：相同key的第一个数据和初始值进行分区内计算，分区内和分区间计算规则可以不相同
- CombineByKey:当计算时，发现数据结构不满足要求时，可以让第一个数据转换结构。分区内和分区间计算规则不相同。

```scala
object Spark14_RDD_Operator_Transform_totalByKey {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - (Key - Value类型)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ),2)

    /*
    reduceByKey:

         combineByKeyWithClassTag[V](
             (v: V) => v, // 第一个值不会参与计算
             func, // 分区内计算规则
             func, // 分区间计算规则
             )

    aggregateByKey :

        combineByKeyWithClassTag[U](
            (v: V) => cleanedSeqOp(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
            cleanedSeqOp, // 分区内计算规则
            combOp,       // 分区间计算规则
            )

    foldByKey:

        combineByKeyWithClassTag[V](
            (v: V) => cleanedFunc(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
            cleanedFunc,  // 分区内计算规则
            cleanedFunc,  // 分区间计算规则
            )

    combineByKey :

        combineByKeyWithClassTag(
            createCombiner,  // 相同key的第一条数据进行的处理函数
            mergeValue,      // 表示分区内数据的处理函数
            mergeCombiners,  // 表示分区间数据的处理函数
            )

     */

    rdd.reduceByKey(_+_) // wordcount
    rdd.aggregateByKey(0)(_+_, _+_) // wordcount
    rdd.foldByKey(0)(_+_) // wordcount
    rdd.combineByKey(v=>v,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y) // wordcount

    sc.stop()

  }
}
```

#### 7 join

```scala
object Spark14_RDD_Operator_Transform_join {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // join : 两个不同数据源的数据，相同的key的value会连接在一起，形成元组
    //        如果两个数据源中key没有匹配上，那么数据不会出现在结果中
    //        如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长，会导致性能降低。

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 9), ("b", 22), ("c", 33)))
    val resRdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    
    resRdd.collect().foreach(println)
      
    sc.stop()
  }
}
```

#### 8 leftOuterJoin

```scala
object Spark14_RDD_Operator_Transform_rightOuterJoin {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 9), ("b", 22)))
    rdd1.leftOuterJoin(rdd2).collect().foreach(println)
    sc.stop()

  }
}
```

### 案例实操

1. 数据准备

   agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。

2. 需求描述：

   统计出每一个省份每个广告被点击数量排行的Top3

3. 需求分析

![image-20210711183952646](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711183952646.png)

4. 代码实现

```scala
package com.xupt.rdd.operator.transform.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 1. 获取原始数据：时间戳，省份，城市，用户，广告
    val rdd: RDD[String] = sc.textFile("datas/agent.log")

    // 2. 获取省份 广告 和 点击数，转化为 ((省份, 广告),1)
    val mapRdd: RDD[((String, String), Int)] = rdd.map(line => {
      val data: Array[String] = line.split(" ")
      ((data(1), data(4)), 1)
    })

    // 3. 按照省份和广告聚合
    //    ( ( 省份，广告 ), 1 ) => ( ( 省份，广告 ), sum )
    val reduceRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_+_)

    // 4. 将聚合的结果进行结构的转换
    //    ( ( 省份，广告 ), sum ) => ( 省份, ( 广告, sum ) )
    val newMapRdd: RDD[(String, (String, Int))] = reduceRdd.map {
      case ((pro, ad), sum) => {
        (pro, (ad, sum))
      }
    }

    // 5. 将转换结构后的数据根据省份进行分组
    //    ( 省份, 【( 广告A, sumA )，( 广告B, sumB )】 )
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = newMapRdd.groupByKey()

    // 6. 将分组后的数据组内排序（降序），取前3名
    val resRdd: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(
      ite => ite.toList.sortBy(_._2)(Ordering.Int.reverse)
    )

    // 7. 打印到控制台
    resRdd.collect().foreach(println)

    sc.stop()
  }
}
```

## 行动算子

所谓的行动算子，其实就是触发作业(Job)执行的方法，底层代码调用的是环境对象的runJob方法，底层代码中会创建ActiveJob，并提交执行。

```scala
object Spark14_RDD_Operator_Transform_action {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    
    // TODO - 行动算子
    // 所谓的行动算子，其实就是触发作业(Job)执行的方法
    // 底层代码调用的是环境对象的runJob方法
    // 底层代码中会创建ActiveJob，并提交执行。
    
    rdd.collect().foreach(println)
    sc.stop()

  }
}
```

### 1 reduce & collect & count & first & take & takeOrdered

```scala
object Spark2_RDD_Operator_Transform_reduce {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 1.reduce
    val res: Int = rdd.reduce(_+_)
    println(res)

    // 2. collect
    val array: Array[Int] = rdd.collect()
    println(array.mkString(","))

    // 3. count
    val count: Long = rdd.count()
    println(count)

    // 4.first
    val first: Int = rdd.first()
    println(first)

    // 5. take
    val take: Array[Int] = rdd.take(3)
    println(take.mkString(","))

    // takeOrdered : 数据排序后，取N个数据
    val rdd1 = sc.makeRDD(List(4,2,3,1))
    val ints1: Array[Int] = rdd1.takeOrdered(3)
    println(ints1.mkString(","))

    sc.stop()
  }
}
```

### 2 aggregate

分区的数据通过初始值和**分区内**的数据进行聚合，然后再和初始值进行**分区间**的数据聚合

```scala
object Spark3_RDD_Operator_Transform_aggregate {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // aggregateByKey : 初始值只会参与分区内计算
    // aggregate : 初始值会参与分区内计算,并且和参与分区间计算
    val res: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(res)
    sc.stop()
  }
}
```

 ### 3 folder

```scala
object Spark4_RDD_Operator_Transform_folder {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val res: Int = rdd.fold(10)(_ + _)
    println(res)
    sc.stop()
  }
}
```

### 4 countByKey

```scala
object Spark5_RDD_Operator_Transform_countbykey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    //    println(intToLong)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("b", 2), ("b", 2), ("c", 2)))
    val stringToLong: collection.Map[String, Long] = rdd.countByKey()
    println(stringToLong)

    sc.stop()
  }
}
```

### 5 save

将数据保存到不同格式的文件中

```scala
object Spark5_RDD_Operator_Transform_save {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output")

    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a",2),("b",3)))

    // saveAsSequenceFile方法要求数据的格式必须为K-V类型
    rdd2.saveAsSequenceFile("output")

    sc.stop()
  }
}
```

### 6 foreach

分布式遍历RDD中的每一个元素，调用指定函数

```scala
object Spark7_RDD_Operator_Transform_Foreach {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // foreach 其实是Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)

    println("**********")

    // foreach 其实是Executor端内存数据打印

    // 算子 ： Operator（操作）
    //         RDD的方法和Scala集合对象的方法不一样
    //         集合对象的方法都是在同一个节点的内存中完成的。
    //         RDD的方法可以将计算逻辑发送到Executor端（分布式节点）执行
    //         为了区分不同的处理效果，所以将RDD的方法称之为算子。
    //        RDD的方法外部的操作都是在Driver端执行的，而方法内部的逻辑代码是在Executor端执行。

    rdd.foreach(println)

    sc.stop()
  }
}
```

闭包检测：RDD算子中传递的函数是会包含闭包操作，那么就会进行检测功能

```scala
package com.xupt.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark7_RDD_Operator_Transform_Foreach1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val user: User = new User()

    // SparkException: Task not serializable
    // NotSerializableException: com.atguigu.bigdata.spark.core.rdd.operator.action.Spark07_RDD_Operator_Action$User

    // RDD算子中传递的函数是会包含闭包操作，那么就会进行检测功能
    // 闭包检测
    rdd.foreach(num => {
      println(s"age is :${user.age + num}")
    })

    sc.stop()
  }
}

//class User extends Serializable {

// 样例类在编译时，会自动混入序列化特质（实现可序列化接口）
case class User(){
  val age: Int = 10
}
```

## RDD 序列化

### 1 闭包检查

从计算的角度, ==算子以外的代码都是在Driver端执行, 算子里面的代码都是在Executor端执行==。那么在scala的函数式编程中，就会导致**算子内**经常会用到**算子外**的数据，这样就形成了闭包的效果，如果使用的算子外的数据无法序列化，就意味着无法传值给Executor端执行，就会发生错误，所以需要在执行任务计算前，检测闭包内的对象是否可以进行序列化，这个操作我们称之为闭包检测。Scala2.12版本后闭包编译方式发生了改变

### 2 序列化方法和属性

从计算的角度, 算子以外的代码都是在Driver端执行, 算子里面的代码都是在Executor端执行，看如下代码：

```scala
package com.atguigu.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

        val search = new Search("h")

        //search.getMatch1(rdd).collect().foreach(println)
        search.getMatch2(rdd).collect().foreach(println)

        sc.stop()
    }
    // 查询对象
    // 类的构造参数其实是类的属性, 构造参数需要进行闭包检测，其实就等同于类进行闭包检测
    class Search(query:String){

        def isMatch(s: String): Boolean = {
            s.contains(this.query)
        }

        // 函数序列化案例
        def getMatch1 (rdd: RDD[String]): RDD[String] = {
            rdd.filter(isMatch)
        }

        // 属性序列化案例
        def getMatch2(rdd: RDD[String]): RDD[String] = {
            val s = query
            rdd.filter(x => x.contains(s))
        }
    }
}
```

### 3 Kryo序列化框架

参考地址: https://github.com/EsotericSoftware/kryo
Java的序列化能够序列化任何的类。但是比较重（字节多），序列化后，对象的提交也比较大。Spark出于性能的考虑，Spark2.0开始支持另外一种Kryo序列化机制。Kryo速度是Serializable的10倍。当RDD在Shuffle数据的时候，简单数据类型、数组和字符串类型已经在Spark内部使用Kryo来序列化。
注意：即使使用Kryo序列化，也要继承Serializable接口。

```scala
package com.xupt.rdd.seria

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife 
  */
object Spark02_RDD_Seria {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Search]))
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val search = new Search("h")

    search.getMatch1(rdd).collect().foreach(println)
//    search.getMatch2(rdd).collect().foreach(println)

    sc.stop()
  }

  // 查询对象
  //  class Search(query:String) extends Serializable {
  // 类的构造参数其实是类的属性, 构造参数需要进行闭包检测，其实就等同于类进行闭包检测
  class Search(query: String) extends Serializable {

    def isMatch(s: String): Boolean = {
      s.contains(this.query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      val s = query
      rdd.filter(x => x.contains(s))
    }
  }

}

```

## RDD 依赖关系

### 1 RDD 血缘关系

RDD只支持粗粒度转换，即在大量记录上执行的单个操作。将创建RDD的一系列Lineage（血统）记录下来，以便恢复丢失的分区。RDD的Lineage会记录RDD的元数据信息和转换行为，当该RDD的部分分区数据丢失时，它可以根据这些信息来**重新运算**和恢复丢失的数据分区。



![image-20210711215823218](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711215823218.png)

![image-20210711215747701](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711215747701.png)

![image-20210711220021788](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711220021788.png)

代码示例：

```scala
package com.xupt.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Dep {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        val lines: RDD[String] = sc.textFile("datas/word.txt")
        println(lines.toDebugString)
        println("*************************")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.toDebugString)
        println("*************************")
        val wordToOne = words.map(word=>(word,1))
        println(wordToOne.toDebugString)
        println("*************************")
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        println(wordToSum.toDebugString)
        println("*************************")
        val array: Array[(String, Int)] = wordToSum.collect()
        array.foreach(println)

        sc.stop()

    }
}
```

输出：

```scala
(1) datas/word.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_Dep.scala:13 []
 |  datas/word.txt HadoopRDD[0] at textFile at Spark01_RDD_Dep.scala:13 []
*************************
(1) MapPartitionsRDD[2] at flatMap at Spark01_RDD_Dep.scala:16 []
 |  datas/word.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_Dep.scala:13 []
 |  datas/word.txt HadoopRDD[0] at textFile at Spark01_RDD_Dep.scala:13 []
*************************
(1) MapPartitionsRDD[3] at map at Spark01_RDD_Dep.scala:19 []
 |  MapPartitionsRDD[2] at flatMap at Spark01_RDD_Dep.scala:16 []
 |  datas/word.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_Dep.scala:13 []
 |  datas/word.txt HadoopRDD[0] at textFile at Spark01_RDD_Dep.scala:13 []
*************************
(1) ShuffledRDD[4] at reduceByKey at Spark01_RDD_Dep.scala:22 []
 +-(1) MapPartitionsRDD[3] at map at Spark01_RDD_Dep.scala:19 []
    |  MapPartitionsRDD[2] at flatMap at Spark01_RDD_Dep.scala:16 []
    |  datas/word.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_Dep.scala:13 []
    |  datas/word.txt HadoopRDD[0] at textFile at Spark01_RDD_Dep.scala:13 []
*************************
(spark,1)
(Hello,2)
(world,1)
```

### 2 RDD 依赖关系

这里所谓的依赖关系，其实就是两个相邻RDD之间的关系

```scala
package com.xupt.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Dep {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        val lines: RDD[String] = sc.textFile("datas/word.txt")
        println(lines.dependencies)
        println("*************************")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.dependencies)
        println("*************************")
        val wordToOne = words.map(word=>(word,1))
        println(wordToOne.dependencies)
        println("*************************")
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        println(wordToSum.dependencies)
        println("*************************")
        val array: Array[(String, Int)] = wordToSum.collect()
        array.foreach(println)

        sc.stop()

    }
}
```

输出：

```scala
List(org.apache.spark.OneToOneDependency@799ed4e8)
*************************
List(org.apache.spark.OneToOneDependency@712cfb63)
*************************
List(org.apache.spark.OneToOneDependency@a098d76)
*************************
List(org.apache.spark.ShuffleDependency@48df4071)
*************************
(spark,1)
(Hello,2)
(world,1)
```

### 3 窄依赖

窄依赖表示每一个父(上游)RDD的Partition最多被子（下游）RDD的一个Partition使用，窄依赖我们形象的比喻为独生子女。

```scala
class OneToOneDependency[T](rdd : org.apache.spark.rdd.RDD[T]) extends org.apache.spark.NarrowDependency[T]
```

![image-20210711222347625](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711222347625.png)

![image-20210711223004212](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711223004212.png)

### 4 宽依赖

宽依赖表示同一个父（上游）RDD的Partition被多个子（下游）RDD的Partition依赖，会引起Shuffle，总结：宽依赖我们形象的比喻为多生。

```scala
class ShuffleDependency[K, V, C]
```

![image-20210711222541472](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711222541472.png)

![image-20210711223102434](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711223102434.png)

### 5 RDD 阶段划分

DAG（Directed Acyclic Graph）有向无环图是由点和线组成的拓扑图形，该图形具有方向，不会闭环。例如，DAG记录了RDD的转换过程和任务的阶段。

![image-20210711231323008](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711231323008.png)

![image-20210711231413116](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711231413116.png)

### 6 RDD任务划分

RDD任务切分中间分为：Application、Job、Stage和Task

⚫ Application：初始化一个SparkContext即生成一个Application；
⚫ Job：一个Action算子就会生成一个Job；
⚫ Stage：Stage等于宽依赖(ShuffleDependency)的个数加1；
⚫ Task：一个Stage阶段中，最后一个RDD的分区个数就是Task的个数。

**注意：Application->Job->Stage->Task每一层都是1对n的关系。**

![image-20210712230906296](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210712230906296.png)

![image-20210712224836678](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210712224836678.png)

## RDD持久化

### 1 RDD缓存

RDD通过Cache或者Persist方法将前面的计算结果缓存，默认情况下会把数据以缓存在JVM的堆内存中。但是并不是这两个方法被调用时立即缓存，而是触发后面的action算子时，该RDD将会被缓存在计算节点的内存中，并供后面重用。

缓存有可能丢失，或者存储于内存的数据由于内存不足而被删除，RDD的缓存容错机制保证了即使缓存丢失也能保证计算的正确执行。通过基于RDD的一系列转换，丢失的数据会被重算，由于RDD的各个Partition是相对独立的，因此只需要计算丢失的部分即可，并不需要重算全部Partition。

Spark会自动对一些Shuffle操作的中间数据做持久化操作(比如：reduceByKey)。这样做的目的是为了当一个节点Shuffle失败了避免重新计算整个输入。但是，在实际使用的时候，如果想重用数据，仍然建议调用persist或cache。

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word => {
      println("@@@@@@@@@@@@")
      (word, 1)
    })
    // cache默认持久化的操作，只能将数据保存到内存中，如果想要保存到磁盘文件，需要更改存储级别
    // mapRDD.cache()

    // 持久化操作必须在行动算子执行时完成的。
    mapRDD.persist(StorageLevel.DISK_ONLY)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("**************************************")
    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
```

### 2 RDD CheckPoint检查点

所谓的检查点其实就是通过将RDD中间结果写入磁盘

由于血缘依赖 过长会造成容错本高，这样就不如在中间阶段做检查点容错，如果检测点之后节点出现问题，可以从检测点做血缘，减少了开销。

对RDD做checkpoint操作不会马上执行，必须ActionAction ActionAction 操作才能触发。

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
        val sc = new SparkContext(sparConf)
        // 设置的保存路径
        sc.setCheckpointDir("cp")

        val list = List("Hello Scala", "Hello Spark")

        val rdd = sc.makeRDD(list)

        val flatRDD = rdd.flatMap(_.split(" "))

        val mapRDD = flatRDD.map(word=>{
            println("@@@@@@@@@@@@")
            (word,1)
        })
        // checkpoint 需要落盘，需要指定检查点保存路径
        // 检查点路径保存的文件，当作业执行完毕后，不会被删除
        // 一般保存路径都是在分布式存储系统：HDFS
        // checkpoint会导致从数据源再执行一遍
        mapRDD.checkpoint()

        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        reduceRDD.collect().foreach(println)
        println("**************************************")
        val groupRDD = mapRDD.groupByKey()
        groupRDD.collect().foreach(println)


        sc.stop()
    }
}

```

### 3 缓存和检查点区别

1）Cache缓存只是将数据保存起来，不切断血缘依赖。Checkpoint检查点切断血缘依赖。
2）Cache缓存的数据通常存储在磁盘、内存等地方，可靠性低。Checkpoint的数据通常存储在HDFS等容错、高可用的文件系统，可靠性高。
3）建议对checkpoint()的RDD使用Cache缓存，这样checkpoint的job只需从Cache缓存中读取数据即可，否则需要再从头计算一次RDD。

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Persist {

    def main(args: Array[String]): Unit = {

        // cache : 将数据临时存储在内存中进行数据重用
        //         会在血缘关系中添加新的依赖。一旦，出现问题，可以重头读取数据
        // persist : 将数据临时存储在磁盘文件中进行数据重用
        //           涉及到磁盘IO，性能较低，但是数据安全
        //           如果作业执行完毕，临时保存的数据文件就会丢失
        // checkpoint : 将数据长久地保存在磁盘文件中进行数据重用
        //           涉及到磁盘IO，性能较低，但是数据安全
        //           为了保证数据安全，所以一般情况下，会独立执行作业
        //           为了能够提高效率，一般情况下，是需要和cache联合使用
        //           执行过程中，会切断血缘关系。重新建立新的血缘关系
        //           checkpoint等同于改变数据源

        val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
        val sc = new SparkContext(sparConf)
        sc.setCheckpointDir("cp")

        val list = List("Hello Scala", "Hello Spark")

        val rdd = sc.makeRDD(list)

        val flatRDD = rdd.flatMap(_.split(" "))

        val mapRDD = flatRDD.map(word=>{
            (word,1)
        })
        //mapRDD.cache()
        mapRDD.checkpoint()
        println(mapRDD.toDebugString)
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        reduceRDD.collect().foreach(println)
        println("**************************************")
        println(mapRDD.toDebugString)

        sc.stop()
    }
}
```

## RDD分区器

Spark目前支持Hash分区和Range分区，和用户自定义分区。Hash分区为当前的默认分区。分区器直接决定了RDD中分区的个数、RDD中每条数据经过Shuffle后进入哪个分区，进而决定了Reduce的个数。

- 只有Key-Value类型的RDD才有分区器，非Key-Value类型的RDD分区的值是None
- 每个RDD的分区ID范围：0 ~ (numPartitions - 1)，决定这个值是属于那个分区的。

![image-20210714232509584](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210714232509584.png)

### 1 Hash分区：对于给定的key，计算其hashCode,并除以分区个数取余

```scala
class HashPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
```

### 2 Range分区：将一定范围内的数据映射到一个分区中，尽量保证每个分区数据均匀，而且分区间有序。

```scala
class RangePartitioner[K : Ordering : ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    private var ascending: Boolean = true,
    val samplePointsPerPartitionHint: Int = 20)
  extends Partitioner {

  // A constructor declared in order to maintain backward compatibility for Java, when we add the
  // 4th constructor parameter samplePointsPerPartitionHint. See SPARK-22160.
  // This is added to make sure from a bytecode point of view, there is still a 3-arg ctor.
  def this(partitions: Int, rdd: RDD[_ <: Product2[K, V]], ascending: Boolean) = {
    this(partitions, rdd, ascending, samplePointsPerPartitionHint = 20)
  }

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")
  require(samplePointsPerPartitionHint > 0,
    s"Sample points per partition must be greater than 0 but found $samplePointsPerPartitionHint")

  private var ordering = implicitly[Ordering[K]]

  // An array of upper bounds for the first (partitions - 1) partitions
  private var rangeBounds: Array[K] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      // Cast to double to avoid overflowing ints or longs
      val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        RangePartitioner.determineBounds(candidates, math.min(partitions, candidates.size))
      }
    }
  }

  def numPartitions: Int = rangeBounds.length + 1

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_, _] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeBoolean(ascending)
        out.writeObject(ordering)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ascending = in.readBoolean()
        ordering = in.readObject().asInstanceOf[Ordering[K]]
        binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}
```

### 3 自定义分区

```scala
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author Wnlife 
  */
object Spark_01_RDD_Partitioner {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "123456"), ("cba", "93567"), ("gba", "123456"), ("nba", "123456")
    ), 3)

    val parRdd: RDD[(String, String)] = rdd.partitionBy(new myPartitioner())
    parRdd.saveAsTextFile("output")
  }

  /**
    * 自定义分区器
    * 1. 继承Partitioner
    * 2. 重写方法
    */
  class myPartitioner extends Partitioner {
    // 分区数量
    override def numPartitions: Int = 3
    // 根据数据的key值返回数据所在的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }
    }
  }
}
```

## RDD文件读取与保存

Spark的数据读取及数据保存可以从两个维度来作区分：文件格式以及文件系统。
文件格式分为：text文件、csv文件、sequence文件以及Object文件；
文件系统分为：本地文件系统、HDFS、HBASE以及数据库。

- text文件
- sequence文件
  - SequenceFile文件是Hadoop用来存储二进制形式的key-value对而设计的一种平面文件(Flat File)。在SparkContext中，可以调用sequenceFile[keyClass, valueClass](path)。
- object对象文件
  - 对象文件是将对象序列化后保存的文件，采用Java的序列化机制。可以通过objectFile[T: ClassTag](path)函数接收一个路径，读取对象文件，返回对应的RDD，也可以通过调用saveAsObjectFile()实现对对象文件的输出。因为是序列化所以要指定类型。

保存：

```scala
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Save {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(
      List(
        ("a", 1),
        ("b", 2),
        ("c", 3)
      )
    )

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")

    sc.stop()
  }
}
```

读取：

```scala
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Load {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd = sc.textFile("output1")
    println(rdd.collect().mkString(","))

    val rdd1 = sc.objectFile[(String, Int)]("output2")
    println(rdd1.collect().mkString(","))

    val rdd2 = sc.sequenceFile[String, Int]("output3")
    println(rdd2.collect().mkString(","))

    sc.stop()
  }
}
```

# 二、Spark累加器

## 1 实现原理

累加器用来把Executor端变量信息聚合到Driver端。在Driver程序中定义的变量，在Executor端的每个Task都会得到这个变量的一份新的副本，每个task更新这些副本的值后，传回Driver端进行merge。

![image-20210716195614247](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210716195614247.png)

## 2 系统累加器

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    // 获取系统累加器
    // Spark默认就提供了简单数据聚合的累加器
    // 累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    //    sc.doubleAccumulator
    //sc.collectionAccumulator

    rdd.foreach(v => {
      // 使用累加器
      sumAcc.add(v)
    })
    // 获取累加器的值
    println(sumAcc.value)

    sc.stop()
  }
}
```

累加器的使用注意：一般情况下，累加器会放置在行动算子进行操作

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    // 获取系统累加器
    // Spark默认就提供了简单数据聚合的累加器
    // 累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    //    sc.doubleAccumulator
    //sc.collectionAccumulator

    val value: RDD[Int] = rdd.map(v => {
      // 使用累加器
      sumAcc.add(v)
      v
    })

    // 获取累加器的值
    // 少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 多加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 一般情况下，累加器会放置在行动算子进行操作

    value.collect()
    value.collect()

    // 获取累加器的值
    println(sumAcc.value)

    sc.stop()
  }
}
```

## 3 自定义累加器

累加器实现wordcount

```scala
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_Acc_WordCount {
  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List("hello", "spark", "hello"))

    // 累加器 : WordCount
    // 创建累加器对象
    val wcAcc = new MyAccumulator()

    // 向Spark进行注册
    sc.register(wcAcc, "wordCountAcc")

    rdd.foreach(
      word => {
        // 数据的累加（使用累加器）
        wcAcc.add(word)
      }
    )

    // 获取累加器累加的结果
    println(wcAcc.value)

    sc.stop()

  }

  /*
    自定义数据累加器：WordCount

    1. 继承AccumulatorV2, 定义泛型
       IN : 累加器输入的数据类型 String
       OUT : 累加器返回的数据类型 mutable.Map[String, Long]

    2. 重写方法（6）
   */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    private var wcMap = mutable.Map[String, Long]()

    // 判断是否初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    // 获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCnt)
    }

    // Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {

      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach {
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}
```

# 三、广播变量

## 1 实现原理

广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用。比如，如果你的应用需要向所有节点发送一个较大的只读查询表，广播变量用起来都很顺手。在多个并行操作中使用同一个变量，但是 Spark会为每个任务分别发送。

![image-20210716214441584](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210716214441584.png)

## 2 代码变量

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark02_Acc {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    // join会导致数据量几何增长，并且会影响shuffle的性能，不推荐使用
    //    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    //    val resRdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    // (a, 1),    (b, 2),    (c, 3)
    // (a, (1,4)),(b, (2,5)),(c, (3,6))
    rdd1.map {
      case (w, c) => {
        val newCount: Int = map.getOrElse(w, 0)
        (w, (c, newCount))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
```

```scala
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark02_Bc {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    // 封装广播变量
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    val mapBr: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    // (a, 1),    (b, 2),    (c, 3)
    // (a, (1,4)),(b, (2,5)),(c, (3,6))
    rdd1.map {
      case (w, c) => {
        // 方法广播变量
        val newCount: Int = mapBr.value.getOrElse(w, 0)
        (w, (c, newCount))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
```