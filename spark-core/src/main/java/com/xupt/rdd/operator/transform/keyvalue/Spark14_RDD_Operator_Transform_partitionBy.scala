package com.xupt.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform_partitionBy {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)),2)
        // RDD => PairRDDFunctions
        // 隐式转换（二次编译）

        // partitionBy根据指定的分区规则对数据进行重分区

        val resRdd: RDD[(String, Int)] = rdd.partitionBy(new HashPartitioner(2))
        resRdd.saveAsTextFile("output")
        sc.stop()

    }
}
