package com.xupt.rdd.operator.transform.map

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * map 算子
  * 小功能：从服务器日志数据apache.log中获取用户请求URL资源路径
  */
object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)
    val logRdd: RDD[String] = sc.textFile("datas/apache.log")
    val urlRdd: RDD[String] = logRdd.map(line => {
      val splitRdd: Array[String] = line.split(" ")
      splitRdd(6)
    })
    urlRdd.collect().foreach(println)
    sc.stop()
  }
}
