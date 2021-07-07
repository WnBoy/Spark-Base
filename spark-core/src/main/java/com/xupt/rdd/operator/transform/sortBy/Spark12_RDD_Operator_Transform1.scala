package com.xupt.rdd.operator.transform.sortBy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *         distinct算子
  */
object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
    val resRdd: RDD[(String, Int)] = rdd.sortBy(t => t._1, false)

    // sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式
    // sortBy默认情况下，不会改变分区。但是中间存在shuffle操作

    resRdd.collect().mkString(",").foreach(println)
    sc.stop()
  }
}
