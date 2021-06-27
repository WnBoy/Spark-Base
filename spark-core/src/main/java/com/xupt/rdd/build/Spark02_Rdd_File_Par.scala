package com.xupt.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife 
  */
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
    // 分区数量的计算方式：
    //    totalSize = 7
    //    goalSize =  7 / 2 = 3（byte）
    //    hadoop读取文件的时候，有个1.1原则，如果剩余的文件大小大于10%，则创建一个新的分区，否则将剩余的数据放到前面的分区中
    //    7 / 3 = 2...1 (1.1) + 1 = 3(分区)

    val fileRdd: RDD[String] = sc.textFile("datas/6.txt",2)

    fileRdd.saveAsTextFile("output")

    sc.stop()
  }
}
