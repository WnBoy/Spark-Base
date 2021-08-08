package com.xupt.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * @author Wnlife 
  */
object SparkStreaming05_State {
  def main(args: Array[String]): Unit = {
    // TODO 创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")
    // TODO 逻辑处理
    // 获取端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordToOne: DStream[(String, Int)] = words.map((_, 1))

    //    val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)
    // updateStateByKey：根据key对数据的状态进行更新
    // 传递的参数中含有两个值
    // 第一个值表示相同的key的value数据
    // 第二个值表示缓存区相同key的value数据
    val wordToCount: DStream[(String, Int)] = wordToOne.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val wordCount: Int = buff.getOrElse(0) + seq.sum
        Option(wordCount)
      }
    )

    wordToCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
