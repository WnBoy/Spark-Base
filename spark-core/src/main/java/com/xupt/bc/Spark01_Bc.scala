package com.xupt.bc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark02_Acc {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    // join会导致数据量几何增长，并且会影响shuffle的性能，不推荐使用
    //    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    //    val resRdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    // (a, 1),    (b, 2),    (c, 3)
    // (a, (1,4)),(b, (2,5)),(c, (3,6))
    rdd1.map {
      case (w, c) => {
        val newCount: Int = map.getOrElse(w, 0)
        (w, (c, newCount))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
