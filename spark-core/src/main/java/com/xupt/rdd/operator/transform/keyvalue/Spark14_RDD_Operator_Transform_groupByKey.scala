package com.xupt.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform_groupByKey {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3),("b",3)),2)

        // groupByKey : 将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
        //              元组中的第一个元素就是key，
        //              元组中的第二个元素就是相同key的value的集合
        val groupByKeyRdd: RDD[(String, Iterable[Int])] = rdd.groupByKey()

        val groupByRdd: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

        groupByKeyRdd.collect().foreach(println)

//        groupByRdd.collect().foreach(println)
        sc.stop()

    }
}
