package com.xupt.spark.sql.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author Wnlife
  *
  *         RDD <=> DataSet <=> DataFrame  转换需要引入隐式转换规则，否则无法转换
  */
object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //  RDD <=> DataSet <=> DataFrame  转换需要引入隐式转换规则，否则无法转换
    // spark不是包名，是上下文环境对象名
    import spark.implicits._

    //  DataFrame
    val df: DataFrame = spark.read.option("header", true).csv("datas/csv/1.csv")
    //    df.show()
    //  DataFrame => SQL
    //    df.createOrReplaceTempView("user")
    spark.sql("select age from user").show()

    //  DataFrame => DSL
    //  在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    //    df.select("age", "name").show()
    //    df.select($"age" + 1).show()
    //    df.select('age + 2).show()

    // DataSet
    val seq: Seq[Int] = Seq(1, 2, 3, 4, 5, 6)
    val ds: Dataset[Int] = seq.toDS()
    //    ds.show()

    // 3. 三者的互相转换
    //    rdd <=> DataFrame
//        val rdd1: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "谢爽", 26), (2, "哇哇", 25)))
//        val df: DataFrame = rdd1.toDF("id", "name", "age")
    //    df.show()
    //    val rdd2: RDD[Row] = df.rdd

    //  DataFrame <=> DataSet
//        val ds: Dataset[User] = df.as[User]
//        val df2: DataFrame = ds.toDF()

    //  DataSet <=> rdd
    //    val rdd3: RDD[User] = ds.rdd
    //    val ds3: Dataset[User] = rdd1.map(line => User(line._1, line._2, line._3)).toDS()

    spark.close()
  }

  case class User(id: Int, name: String, age: Int)

}
