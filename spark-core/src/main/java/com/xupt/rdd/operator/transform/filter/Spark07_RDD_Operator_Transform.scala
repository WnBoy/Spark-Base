package com.xupt.rdd.operator.transform.filter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  *         filter 算子
  *
  */
object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val filterRdd: RDD[Int] = rdd.filter(num => num % 2 != 0)

    filterRdd.collect().foreach(println)

    sc.stop()
  }
}
