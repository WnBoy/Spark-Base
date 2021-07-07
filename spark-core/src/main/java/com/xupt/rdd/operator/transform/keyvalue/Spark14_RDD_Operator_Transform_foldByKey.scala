package com.xupt.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform_foldByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)

    // 如果聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法
    val resRdd: RDD[(String, Int)] = rdd.foldByKey(0)( _ + _)

    resRdd.collect().foreach(println)

    sc.stop()

  }
}
