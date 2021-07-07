package com.xupt.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform_aggregateByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    /**
      * 分区内求最大值，分区间求和
      */
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)

    // (a,【1,2】), (a, 【3，4】)
    // (a, 2), (a, 4)
    // (a, 6)

    // aggregateByKey存在函数柯里化，有两个参数列表
    // 第一个参数列表,需要传递一个参数，表示为初始值
    //       主要用于当碰见第一个key的时候，和value进行分区内计算
    // 第二个参数列表需要传递2个参数
    //      第一个参数表示分区内计算规则
    //      第二个参数表示分区间计算规则

    val resRdd: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max, _ + _)

    resRdd.collect().foreach(println)

    sc.stop()

  }
}
