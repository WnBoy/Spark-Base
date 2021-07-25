package com.xupt.spark.sql.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql._

/**
  * @author Wnlife
  *
  *         UDAF强类型实现--Spark3.0增加的
  */
object Spark03_SparkSQL_UDAF1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val structType: StructType = StructType(Array(StructField("name", StringType), StructField("age", LongType)))
    val df: DataFrame = spark.read.option("header", true).schema(structType).csv("datas/csv/1.csv")
    df.show()

    spark.udf.register("myAvg", functions.udaf(new MyAvgUDAF()))
    df.createOrReplaceTempView("user")
    spark.sql("select myAvg(age) from user").show()

    spark.close()
  }

  /*
   自定义聚合函数类：计算年龄的平均值
   1. 继承org.apache.spark.sql.expressions.Aggregator, 定义泛型
       IN : 输入的数据类型 Long
       BUF : 缓冲区的数据类型 Buff
       OUT : 输出的数据类型 Long
   2. 重写方法(6)
 */
  case class Buf(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[Long, Buf, Long] {

    // 初始值
    override def zero: Buf = {
      Buf(0L, 0L)
    }

    // 根据输入值更新Buffer
    override def reduce(buf: Buf, a: Long): Buf = {
      buf.total = buf.total + a
      buf.count = buf.count + 1
      buf
    }

    // 合并缓冲区
    override def merge(buf1: Buf, buf2: Buf): Buf = {
      buf1.total = buf1.total + buf2.total
      buf1.count = buf1.count + buf2.count
      buf1
    }

    // 最后的计算结果
    override def finish(buf: Buf): Long = buf.total / buf.count

    // 缓冲区的编码操作
    override def bufferEncoder: Encoder[Buf] = Encoders.product

    // 输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
