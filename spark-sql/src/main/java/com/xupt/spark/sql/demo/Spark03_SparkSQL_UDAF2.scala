package com.xupt.spark.sql.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * @author Wnlife
  *
  *         UDAF强类型实现--spark3.0以前
  */
object Spark03_SparkSQL_UDAF2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val structType: StructType = StructType(Array(StructField("name", StringType), StructField("age", LongType)))
    val df: DataFrame = spark.read.option("header", true).schema(structType).csv("datas/csv/1.csv")
    df.show()

    // 早期版本中，spark不能在sql中使用强类型UDAF操作
    // SQL & DSL
    // 早期的UDAF强类型聚合函数使用DSL语法操作
    val ds: Dataset[User] = df.as[User]

    // 将UDAF函数转换为查询的列对象
    val udafCol: TypedColumn[User, Long] = new MyAvgUDAF().toColumn

    ds.select(udafCol)

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

  case class User(name: String, age: Long)

  case class Buf(var total: Long, var count: Long)

  class MyAvgUDAF extends Aggregator[User, Buf, Long] {

    // 初始值
    override def zero: Buf = {
      Buf(0L, 0L)
    }

    // 根据输入值更新Buffer
    override def reduce(buf: Buf, a: User): Buf = {
      buf.total = buf.total + a.age
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
