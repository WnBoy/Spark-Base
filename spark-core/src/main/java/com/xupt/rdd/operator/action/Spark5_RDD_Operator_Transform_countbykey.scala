package com.xupt.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark5_RDD_Operator_Transform_countbykey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    //    println(intToLong)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("b", 2), ("b", 2), ("c", 2)))
    val stringToLong: collection.Map[String, Long] = rdd.countByKey()
    println(stringToLong)

    sc.stop()
  }
}