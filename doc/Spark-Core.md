# RDD
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
  - 主要用于当碰见第一个key的时候，和value进行分区内计算

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
    rdd1.join(rdd2).collect().foreach(println)
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

==周末看==











