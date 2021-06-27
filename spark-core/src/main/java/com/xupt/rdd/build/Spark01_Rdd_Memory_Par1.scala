package com.xupt.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife 
  */
object Spark01_Rdd_Memory_Par1 {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    sparkConf.set("spark.default.parallelism","2")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 创建RDD
    val seq: Seq[Int] = Seq(1,2,3,4,5)

    // 【1，2】，【3，4】
    //val rdd = sc.makeRDD(List(1,2,3,4), 2)
    // 【1】，【2】，【3，4】
    //val rdd = sc.makeRDD(List(1,2,3,4), 3)
    // 【1】，【2,3】，【4,5】
    val rdd: RDD[Int] = sc.makeRDD(seq,3)

    println(rdd.getNumPartitions) // 获取分区数

    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("output")

    // 关闭资源
    sc.stop()

  }
}
