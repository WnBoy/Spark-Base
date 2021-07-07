package com.xupt.rdd.operator.transform.coalesce

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *         distinct算子
  */
object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    // coalesce方法默认情况下不会将分区的数据打乱重新组合
    // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    // 如果想要让数据均衡，可以进行shuffle处理

//    val coalesecRdd: RDD[Int] = rdd.coalesce(2)
    val coalesecRdd: RDD[Int] = rdd.coalesce(2, true)

    coalesecRdd.saveAsTextFile("output")

    sc.stop()
  }
}
