package com.xupt.rdd.operator.transform.groupby

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * groupBy 算子
  *
  */
object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    // groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    // 相同的key值的数据会放置在一个组中
    def groupFun(num: Int): Boolean = {
      num % 2 == 0
    }

    val groupRdd: RDD[(Boolean, Iterable[Int])] = rdd.groupBy(groupFun)

    groupRdd.collect().foreach(println)

    sc.stop()
  }
}
