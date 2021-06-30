package com.xupt.rdd.operator.transform.flatmap

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * flatMap 算子
  *
  */
object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3,4)))
    val mapRdd: RDD[Int] = rdd.flatMap(list=>list)
    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
