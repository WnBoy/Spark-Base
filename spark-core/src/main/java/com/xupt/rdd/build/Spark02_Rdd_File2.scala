package com.xupt.rdd.build

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife 
  */
object Spark02_Rdd_File2 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc: SparkContext = new SparkContext(sparkConf)

    // textFile是按照行读取，读取的是一个字符串
    // wholeTextFiles是按照文件读取，读取的元组第一个是文件的路径，第二个是文件的内容
    val fileRdd: RDD[(String, String)] = sc.wholeTextFiles("datas/*")

    fileRdd.collect().foreach(println)
    sc.stop()
  }
}
