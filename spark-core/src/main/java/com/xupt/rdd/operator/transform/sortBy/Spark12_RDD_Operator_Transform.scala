package com.xupt.rdd.operator.transform.sortBy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *         distinct算子
  */
object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 6, 2, 4, 3, 5), 2)

    val sortRdd: RDD[Int] = rdd.sortBy(num => num)
    sortRdd.saveAsTextFile("output")

    sc.stop()
  }
}
