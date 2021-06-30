package com.xupt.rdd.operator.transform.mappartitions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  *  mapPartitions 算子
  *  小功能：获取每个数据分区的最大值
  */
object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRdd: RDD[Int] = rdd.mapPartitions(itera => {
      List(itera.max).iterator
    })

    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
