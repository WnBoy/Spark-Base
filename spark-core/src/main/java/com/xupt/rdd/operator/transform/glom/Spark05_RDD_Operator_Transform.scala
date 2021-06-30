package com.xupt.rdd.operator.transform.glom

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * glom 算子
  *
  */
object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val arrRdd: RDD[Array[Int]] = rdd.glom()
    arrRdd.collect().foreach(arr=> println(arr.mkString(",")))
    sc.stop()
  }
}
