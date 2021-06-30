package com.xupt.rdd.operator.transform.glom

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * glom 算子
  *
  * 小功能：计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
  *
  */
object Spark05_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val arrRdd: RDD[Array[Int]] = rdd.glom()
    println(arrRdd.map(arr => arr.max).sum())
    sc.stop()
  }
}
