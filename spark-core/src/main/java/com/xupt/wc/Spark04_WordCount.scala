package com.xupt.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @author Wnlife 
  */
object Spark04_WordCount {
  def main(args: Array[String]): Unit = {
    // 1 建立和Spark的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    // 2 执行业务
    wd7(sparkContext)

    // 3 关闭连接
    sparkContext.stop()

  }

  // 1. groupby
  def wd1(sparkContext: SparkContext): Unit = {
    val rdd: RDD[String] = sparkContext.makeRDD(List("Hello world", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val res: RDD[(String, Int)] = group.mapValues(iter => iter.size)
    res.collect().foreach(println)
  }

  // 2. groupbykey 效率不高
  def wd2(sparkContext: SparkContext): Unit = {
    val rdd: RDD[String] = sparkContext.makeRDD(List("Hello world", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = words.map((_,1))
    val group: RDD[(String, Iterable[Int])] = mapRdd.groupByKey()

    val res: RDD[(String, Int)] = group.mapValues(iter => iter.size)
    res.collect().foreach(println)
  }

  // 3. reduceByKey
  def wd3(sparkContext: SparkContext): Unit = {
    val rdd: RDD[String] = sparkContext.makeRDD(List("Hello world", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = words.map((_,1))
    val res: RDD[(String, Int)] = mapRdd.reduceByKey(_+_)

    res.collect().foreach(println)
  }

  // 4. aggregateByKey
  def wd4(sparkContext: SparkContext): Unit = {
    val rdd: RDD[String] = sparkContext.makeRDD(List("Hello world", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = words.map((_,1))
    val res: RDD[(String, Int)] = mapRdd.aggregateByKey(0)(_+_,_+_)

    res.collect().foreach(println)
  }

  // 5. aggregateByKey
  def wd5(sparkContext: SparkContext): Unit = {
    val rdd: RDD[String] = sparkContext.makeRDD(List("Hello world", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = words.map((_,1))
    val res: RDD[(String, Int)] = mapRdd.foldByKey(0)(_+_)

    res.collect().foreach(println)
  }

  // 6. combineByKey
  def wordcount6(sc : SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_,1))
    val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
      v=>v,
      (x:Int, y) => x + y,
      (x:Int, y:Int) => x + y
    )
  }

  // 7. countByKey
  def wd7(sparkContext: SparkContext): Unit = {
    val rdd: RDD[String] = sparkContext.makeRDD(List("Hello world", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = words.map((_,1))
    val res: collection.Map[String, Long] = mapRdd.countByKey()

    res.foreach(println)
  }

  // 8. countByKey
  def wd8(sparkContext: SparkContext): Unit = {
    val rdd: RDD[String] = sparkContext.makeRDD(List("Hello world", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = words.map((_,1))
    val res: collection.Map[(String, Int), Long] = mapRdd.countByValue()
    res.foreach(println)
  }


  // 9 reduce, aggregate, fold
  def wordcount9(sc : SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))

    // 【（word, count）,(word, count)】
    // word => Map[(word,1)]
    val mapWord = words.map(
      word => {
        mutable.Map[String, Long]((word,1))
      }
    )

    val wordCount = mapWord.reduce(
      (map1, map2) => {
        map2.foreach{
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )

    println(wordCount)
  }

}
