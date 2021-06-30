package com.xupt.rdd.operator.transform.map

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * map 算子
  *
  */
object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    val mapRdd: RDD[Int] = rdd.map(_ * 2)
    mapRdd.collect().foreach(println)

    sc.stop()
  }
}
