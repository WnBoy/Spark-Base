package com.xupt.rdd.operator.transform.flatmap

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * flatMap 算子
  * 小功能：将List(List(1,2),3,List(4,5))进行扁平化操作
  */
object Spark04_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)


    val rdd: RDD[Any] = sc.makeRDD(List(List(1,2),3,List(4,5)))
    val mapRdd: RDD[Any] = rdd.flatMap(date => {
      date match {
        case list: List[_] => list
        case num: Int => List(num)
      }
    })

    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
