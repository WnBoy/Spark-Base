package com.xupt.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife 
  */
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    // 1 建立和Spark的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sparkContext = new SparkContext(sparkConf)
    // 2 执行业务
    //
    val lines: RDD[String] = sparkContext.textFile("datas")
    var words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne = words.map(word => (word, 1))
    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)

    val wordCount = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }


    val array: Array[(String, Int)] = wordCount.collect()
    array.foreach(println)

    // 3 关闭连接
    sparkContext.stop()

  }
}
