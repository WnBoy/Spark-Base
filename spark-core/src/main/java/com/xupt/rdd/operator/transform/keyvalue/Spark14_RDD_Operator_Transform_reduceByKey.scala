package com.xupt.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform_reduceByKey {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3),("b",3)),2)
        // reduceByKey : 相同的key的数据进行value数据的聚合操作
        // scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
        // 【1，2，3】
        // 【3，3】
        // 【6】
        // reduceByKey中如果key的数据只有一个，是不会参与运算的。
        val resRdd: RDD[(String, Int)] = rdd.reduceByKey(_+_)

        resRdd.collect().foreach(println)

        sc.stop()

    }
}
