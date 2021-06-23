package com.xupt.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife 
  */
object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    // 1 建立和Spark的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
     val sparkContext: SparkContext = new SparkContext(sparkConf)
    // 2 执行业务
    val lines: RDD[String] = sparkContext.textFile("datas")
    var words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne = words.map(word => (word, 1))

    // 根据相同的key对value做聚合
    val wordCount = wordToOne.reduceByKey(_ + _)

    val array: Array[(String, Int)] = wordCount.collect()
    array.foreach(println)

    // 3 关闭连接
    sparkContext.stop()

  }
}
