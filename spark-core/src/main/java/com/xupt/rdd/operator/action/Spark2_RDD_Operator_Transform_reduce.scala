package com.xupt.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark2_RDD_Operator_Transform_reduce {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 1.reduce
    val res: Int = rdd.reduce(_+_)
    println(res)

    // 2. collect
    val array: Array[Int] = rdd.collect()
    println(array.mkString(","))

    // 3. count
    val count: Long = rdd.count()
    println(count)

    // 4.first
    val first: Int = rdd.first()
    println(first)

    // 5. take
    val take: Array[Int] = rdd.take(3)
    println(take.mkString(","))

    // takeOrdered : 数据排序后，取N个数据
    val rdd1 = sc.makeRDD(List(4,2,3,1))
    val ints1: Array[Int] = rdd1.takeOrdered(3)
    println(ints1.mkString(","))


    sc.stop()
  }
}
