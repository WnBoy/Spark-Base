package com.xupt.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife 
  */
object Spark02_Rdd_File {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc: SparkContext = new SparkContext(sparkConf)
    val fileRdd: RDD[String] = sc.textFile("datas/*")

    fileRdd.collect().foreach(println)
    sc.stop()
  }
}
