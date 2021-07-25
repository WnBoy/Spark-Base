package com.xupt.spark.sql.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
  * @author Wnlife
  *
  */
object Spark04_SparkSQL_CSV {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val df: DataFrame = spark.read.option("header", true).option("seq", ",").option("inferSchema", "true").csv("datas/csv/1.csv")
    df.printSchema()
    df.show()

    df.write.mode(SaveMode.Append).csv("output")

    spark.close()
  }
}
