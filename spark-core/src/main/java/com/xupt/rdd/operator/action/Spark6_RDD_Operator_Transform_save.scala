package com.xupt.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark6_RDD_Operator_Transform_save {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output")

    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a",2),("b",3)))

    // saveAsSequenceFile方法要求数据的格式必须为K-V类型
    rdd2.saveAsSequenceFile("output")

    sc.stop()
  }
}