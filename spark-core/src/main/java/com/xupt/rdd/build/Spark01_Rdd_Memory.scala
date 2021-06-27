package com.xupt.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife 
  */
object Spark01_Rdd_Memory {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 创建RDD
    val seq: Seq[Int] = Seq(1,2,3,4)

    // 方式1
    val rdd_01: RDD[Int] = sc.parallelize(seq)
    // 方式2 ：底层还是调用方式1，推荐使用
    val rdd_02: RDD[Int] = sc.makeRDD(seq)


    rdd_01.foreach(println)
    println("---------------")
    rdd_02.foreach(println)

    // 关闭资源
    sc.stop()

  }
}
