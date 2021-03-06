package com.xupt.bc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark02_Bc {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    // 封装广播变量
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    val mapBr: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    // (a, 1),    (b, 2),    (c, 3)
    // (a, (1,4)),(b, (2,5)),(c, (3,6))
    rdd1.map {
      case (w, c) => {
        // 方法广播变量
        val newCount: Int = mapBr.value.getOrElse(w, 0)
        (w, (c, newCount))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
