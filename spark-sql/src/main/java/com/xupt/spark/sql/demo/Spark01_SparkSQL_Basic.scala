package com.xupt.spark.sql.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author Wnlife 
  */
object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // DataFrame
    val df: DataFrame = spark.read.option("header",true).csv("datas/csv/1.csv")
//    df.show()
    // DataFrame => SQL
//    df.createOrReplaceTempView("user")
//    spark.sql("select age from user").show()

    // DataFrame => DSL
    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    df.select($"age"+1).show()
    df.select('age+2).show()



    spark.close()


  }
}
