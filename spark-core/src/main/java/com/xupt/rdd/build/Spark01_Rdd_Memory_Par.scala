package com.xupt.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife 
  */
object Spark01_Rdd_Memory_Par {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    sparkConf.set("spark.default.parallelism","2")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 创建RDD
    val seq: Seq[Int] = Seq(1,2,3,4)

    /**
      * RDD的并行度 & 分区
      * makeRDD方法可以传递第二个参数，这个参数表示分区的数量
      * 第二个参数可以不传递的，那么makeRDD方法会使用默认值 ： defaultParallelism（默认并行度
      * 默认的并行度：scheduler.conf.getInt("spark.default.parallelism", totalCores)
      * spark在默认情况下，从配置对象中获取配置参数：spark.default.parallelism
      * 如果获取不到，那么使用totalCores属性，这个属性取值为当前运行环境的最大可用核数
      */

        val rdd: RDD[Int] = sc.makeRDD(seq)
//    val rdd: RDD[Int] = sc.makeRDD(seq,6)

    println(rdd.getNumPartitions) // 获取分区数
    rdd.saveAsTextFile("output")
    // 关闭资源
    sc.stop()

  }
}
