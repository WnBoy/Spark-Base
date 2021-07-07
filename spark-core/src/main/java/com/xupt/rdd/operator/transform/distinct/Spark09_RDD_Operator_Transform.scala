package com.xupt.rdd.operator.transform.distinct

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *         distinct算子
  */
object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val data: RDD[Int] = sc.makeRDD(List(1,2,3,4,1,2,3,4))
    // 原理：
    // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)

    // (1, null),(2, null),(3, null),(4, null),(1, null),(2, null),(3, null),(4, null)
    // (1, null)(1, null)(1, null)
    // (null, null) => null
    // (1, null) => 1

    val distinctRdd: RDD[Int] = data.distinct()

    distinctRdd.collect().foreach(println)

    sc.stop()
  }
}
