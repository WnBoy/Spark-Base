package com.xupt.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform_aggregateByKey_Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    /**
      * 获取相同key的数据的平均值 => (a, 3),(b, 4)
      */
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    // aggregateByKey最终的返回数据结果应该和初始值的类型保持一致

    // 获取相同key的数据的平均值 => (a, 3),(b, 4)

    //(0,0)第一个0表示数量，第二个0表示次数
    val aggRdd: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => (t._1 + v, t._2 + 1), (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)
    )
    val resRdd: RDD[(String, Int)] = aggRdd.mapValues {
      case (v1, v2) => {
        v1 / v2
      }
    }

    resRdd.collect().foreach(println)

    sc.stop()
  }
}
