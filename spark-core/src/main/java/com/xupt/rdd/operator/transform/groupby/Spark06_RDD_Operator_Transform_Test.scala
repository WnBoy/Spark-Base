package com.xupt.rdd.operator.transform.groupby

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * groupBy 算子
  *
  */
object Spark06_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    val mapRdd: RDD[(String, Int)] = rdd.map(line => {
      val lineRdd: Array[String] = line.split(" ")
      val time: String = lineRdd(3)
      val dateFormat1: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val date: Date = dateFormat1.parse(time)
      val dateFormat2: SimpleDateFormat = new SimpleDateFormat("HH")
      val houre: String = dateFormat2.format(date)
      (houre, 1)
    })
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd.groupBy(_._1)

    val resRdd: RDD[(String, Int)] = groupRdd.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }
    resRdd.collect().foreach(println)

    sc.stop()
  }
}
