package com.xupt.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife 
  */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // 1 建立和Spark的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sparkContext = new SparkContext(sparkConf)
    // 2 执行业务
    //
    val lines: RDD[String] = sparkContext.textFile("datas")
    var words: RDD[String] = lines.flatMap(_.split(" "))
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount=wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    val array: Array[(String, Int)] = wordCount.collect()
    array.foreach(println)

    // 3 关闭连接
    sparkContext.stop()

  }
}
