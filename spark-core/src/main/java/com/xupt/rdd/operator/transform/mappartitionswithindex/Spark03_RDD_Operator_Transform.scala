package com.xupt.rdd.operator.transform.mappartitionswithindex

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * mapPartitionsWithIndex 算子
  * 小功能：获取第二个数据分区的数据
  *
  */
object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRdd: RDD[Int] = rdd.mapPartitionsWithIndex((index, iterator) => {
      if (index == 1) {
        iterator
      } else {
        Nil.iterator
      }
    })

    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
