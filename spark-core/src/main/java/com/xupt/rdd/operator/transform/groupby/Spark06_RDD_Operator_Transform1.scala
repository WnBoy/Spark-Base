package com.xupt.rdd.operator.transform.groupby

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * groupBy 算子
  *
  */
object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello","Spark","Hadoop","HDFS"),2)

    val groupRdd: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

    groupRdd.collect().foreach(println)

    sc.stop()
  }
}
