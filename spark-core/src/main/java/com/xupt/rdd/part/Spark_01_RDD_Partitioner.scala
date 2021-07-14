package com.xupt.rdd.part

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author Wnlife 
  */
object Spark_01_RDD_Partitioner {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "123456"), ("cba", "93567"), ("gba", "123456"), ("nba", "123456")
    ), 3)

    val parRdd: RDD[(String, String)] = rdd.partitionBy(new myPartitioner())
    parRdd.saveAsTextFile("output")
  }

  /**
    * 自定义分区器
    * 1. 继承Partitioner
    * 2. 重写方法
    */
  class myPartitioner extends Partitioner {
    // 分区数量
    override def numPartitions: Int = 3
    // 根据数据的key值返回数据所在的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }
    }
  }

}
