package com.xupt.rdd.operator.transform.flatmap

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * flatMap 算子
  *
  */
object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello world"))
    val mapRdd: RDD[String] = rdd.flatMap(str=>str.split(" "))

    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
