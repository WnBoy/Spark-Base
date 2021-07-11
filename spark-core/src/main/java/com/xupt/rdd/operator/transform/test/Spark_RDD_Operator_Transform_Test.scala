package com.xupt.rdd.operator.transform.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 1. 获取原始数据：时间戳，省份，城市，用户，广告
    val rdd: RDD[String] = sc.textFile("datas/agent.log")

    // 2. 获取省份 广告 和 点击数，转化为 ((省份, 广告),1)
    val mapRdd: RDD[((String, String), Int)] = rdd.map(line => {
      val data: Array[String] = line.split(" ")
      ((data(1), data(4)), 1)
    })

    // 3. 按照省份和广告聚合
    //    ( ( 省份，广告 ), 1 ) => ( ( 省份，广告 ), sum )
    val reduceRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_+_)

    // 4. 将聚合的结果进行结构的转换
    //    ( ( 省份，广告 ), sum ) => ( 省份, ( 广告, sum ) )
    val newMapRdd: RDD[(String, (String, Int))] = reduceRdd.map {
      case ((pro, ad), sum) => {
        (pro, (ad, sum))
      }
    }

    // 5. 将转换结构后的数据根据省份进行分组
    //    ( 省份, 【( 广告A, sumA )，( 广告B, sumB )】 )
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = newMapRdd.groupByKey()

    // 6. 将分组后的数据组内排序（降序），取前3名
    val resRdd: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(
      ite => ite.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    )

    // 7. 打印到控制台
    resRdd.collect().foreach(println)

    sc.stop()
  }
}
