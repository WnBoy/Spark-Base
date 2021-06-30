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









