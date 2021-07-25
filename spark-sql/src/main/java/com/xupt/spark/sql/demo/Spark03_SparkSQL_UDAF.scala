package com.xupt.spark.sql.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * @author Wnlife
  *
  *         UDAF弱类型实现
  */
object Spark03_SparkSQL_UDAF {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val df: DataFrame = spark.read.option("header", true).csv("datas/csv/1.csv")
    df.show()

    spark.udf.register("myAvg", new MyAvgUDAF())
    df.createOrReplaceTempView("user")
    spark.sql("select myAvg(age) from user").show()

    spark.close()
  }

  /*
 自定义聚合函数类：计算年龄的平均值
 1. 继承UserDefinedAggregateFunction
 2. 重写方法(8)
 */
  class MyAvgUDAF extends UserDefinedAggregateFunction {

    // 输入数据的类型
    override def inputSchema: StructType = {
      StructType(Array(StructField("age", LongType)))
    }

    // 缓冲区数据的类型
    override def bufferSchema: StructType = {
      StructType(Array(StructField("total", LongType), StructField("count", LongType)))
    }

    // 结果计算的类型
    override def dataType: DataType = LongType

    // 函数是否是稳定的
    override def deterministic: Boolean = true

    // 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      //      buffer(0)=0L
      //      buffer(1)=0L

      buffer.update(0, 0L)
      buffer.update(1, 0L)

    }

    // 根据输入的值更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      println(input.toString())
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(1) + 1)
    }

    // 缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    // 计算最终结果
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }

}
