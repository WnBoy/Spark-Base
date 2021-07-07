package com.xupt.rdd.operator.transform.filter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  *         filter 算子
  *
  */
object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)
    val data: RDD[String] = sc.textFile("datas/apache.log")
    val filterRdd: RDD[String] = data.filter(line => {
      val splitRdd: Array[String] = line.split(" ")
      splitRdd(3).startsWith("17/05/2015")
    })

    filterRdd.collect().foreach(println)

    sc.stop()
  }
}
