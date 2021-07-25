package com.xupt.spark.sql.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Wnlife
  *
  *         RDD <=> DataSet <=> DataFrame  转换需要引入隐式转换规则，否则无法转换
  */
object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //  RDD <=> DataSet <=> DataFrame  转换需要引入隐式转换规则，否则无法转换
    // spark不是包名，是上下文环境对象名

    //  DataFrame
    val df: DataFrame = spark.read.option("header", true).csv("datas/csv/1.csv")

    spark.udf.register("prefixName", (name: String) => {
      "Name :" + name
    })

    df.createOrReplaceTempView("user")
    spark.sql("select age, prefixName(name) from user").show()

    spark.close()
  }
}
