package com.xupt.rdd.operator.transform.map

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * map 算子
  *
  */
object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5),2)
    val mapRdd: RDD[Int] = rdd.map(num=>{
      println(num + ">>>>>>>>>>>>")
      num
    })
    val mapRdd2: RDD[Int] = mapRdd.map(num=>{
      println(num + "==========")
      num
    })

//    mapRdd2.collect().foreach(println)

    sc.stop()
  }
}
