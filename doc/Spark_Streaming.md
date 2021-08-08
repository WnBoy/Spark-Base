# 0 基本概念

- 数据处理的方式
  - 流式（Streaming）数据处理：一条一条处理
  - 批量（batch）数据处理：一批一批处理
- 数据处理时间的长短
  - 实时处理数据：毫秒级
  - 离线处理数据：小时 or 天级别

**SparkStreaming 准实时（秒，分钟），微批次（时间）的数据处理框架**

![image-20210808195601916](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808195601916.png)

# 1 概述

## 1.1 Spark Streaming是什么

![image-20210808175233275](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808175233275.png)

![image-20210808175458979](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808175458979.png)

## 1.2 Spark Streaming的特点

- 易用
- 容错
- 易整合到Spark体系

## 1.3 Spark Streaming架构图

![image-20210808180757075](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808180757075.png)

![image-20210808180819957](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808180819957.png)

# 2 DStream入门

## 2.1 WordCount案例实操

![image-20210808180939327](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808180939327.png)

1. 添加依赖

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming_2.12</artifactId>
    <version>${sparkversion}</version>
</dependency>
```

2. 代码：

```scala
package com.xupt.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author Wnlife 
  */
object SparkStream01_WordCount {
  def main(args: Array[String]): Unit = {
    // TODO 创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // TODO 逻辑处理
    // 获取端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map((_, 1))

    val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    wordToCount.print()

    // 由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
    // 如果main方法执行完毕，应用程序也会自动结束。所以不能让main执行完毕
    //ssc.stop()
    // 1. 启动采集器
    ssc.start()
    // 2. 等待采集器的关闭
    ssc.awaitTermination()
  }
}
```

3. 启动程序并通过  netcat发送数据

![image-20210808181140827](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808181140827.png)

![image-20210808181237899](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808181237899.png)

![image-20210808181206980](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808181206980.png)

## 2.2 WordCount解析

![image-20210808181332767](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808181332767.png)

![image-20210808181403106](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808181403106.png)

![image-20210808181419758](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808181419758.png)

![image-20210808181433550](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808181433550.png)

# 3 DSream创建

## 3.1 RDD队列

### 3.1.1 用法及说明

测试过程中，可以通过使用 ssc.queueStream(queueOfRDDs)来创建 DStream，每一个推送到这个队列中的RDD，都会作为一个DStream 处理。

### 3.1.2   案例实操

需求：循环创建几个 RDD，将RDD 放入队列。通过 SparkStream 创建 Dstream，计算WordCount

```scala
package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_Queue {

    def main(args: Array[String]): Unit = {

        // TODO 创建环境对象
        // StreamingContext创建时，需要传递两个参数
        // 第一个参数表示环境配置
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        // 第二个参数表示批量处理的周期（采集周期）
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val rddQueue = new mutable.Queue[RDD[Int]]()

        val inputStream = ssc.queueStream(rddQueue,oneAtATime = false)
        val mappedStream = inputStream.map((_,1))
        val reducedStream = mappedStream.reduceByKey(_ + _)
        reducedStream.print()

        ssc.start()

        for (i <- 1 to 5) {
            rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
            Thread.sleep(2000)
        }

        ssc.awaitTermination()
    }
}
```

## 3.2 自定义数据源

### 3.2.1   用法及说明

需要继承Receiver，并实现 onStart、onStop 方法来自定义数据源采集。

### 3.2.2 案例实操

需求：自定义数据源，获取线程打印内容

```scala
package com.xupt.streaming


import java.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author Wnlife
  */

object SparkStreaming03_DIY {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
    messageDS.print()

    ssc.start()
    ssc.awaitTermination()
  }

  /*
  自定义数据采集器
  1. 继承Receiver，定义泛型, 传递参数
  2. 重写方法
   */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flg = true

    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flg) {
            val message = "采集的数据为：" + new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()
    }

    override def onStop(): Unit = {
      flg = false;
    }
  }
}
```

案例二：自定义数据源，实现监控某个端口号，获取该端口号内容。

1. 自定义数据源

```scala
class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
//最初启动的时候，调用该方法，作用为：读数据并将数据发送给 Spark 
    override def onStart():Unit = {
	new Thread("Socket Receiver") { override def run() {
            receive()
            }
    	}.start()
    }

//读数据并将数据发送给 Spark def receive(): Unit = {

//创建一个 Socket
var socket: Socket = new Socket(host, port)

//定义一个变量，用来接收端口传过来的数据var input: String = null

//创建一个 BufferedReader 用于读取端口传来的数据
val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

//读取数据
input = reader.readLine()

//当 receiver 没有关闭并且输入数据不为空，则循环发送数据给 Spark while (!isStopped() && input != null) {
store(input)
input = reader.readLine()
}

//跳出循环则关闭资源reader.close() socket.close()

//重启任务restart("restart")
}

override def onStop(): Unit = {}
}
```

2. 使用自定义的数据源采集数据

```scala
object FileStream {

def main(args: Array[String]): Unit = {

//1.初始化 Spark 配置信息
val sparkConf = new SparkConf().setMaster("local[*]")
.setAppName("StreamWordCount")

//2.初始化 SparkStreamingContext
val ssc = new StreamingContext(sparkConf, Seconds(5))

//3.创建自定义 receiver 的 Streaming
val lineStream = ssc.receiverStream(new CustomerReceiver("hadoop102", 9999))

//4.将每一行数据做切分，形成一个个单词
val wordStream = lineStream.flatMap(_.split("\t"))

//5.将单词映射成元组（word,1）
val wordAndOneStream = wordStream.map((_, 1))

//6.将相同的单词次数做统计
val wordAndCountStream = wordAndOneStream.reduceByKey(_ + _)

//7.打印wordAndCountStream.print()

//8.启动 SparkStreamingContext ssc.start() ssc.awaitTermination()
}
}
```

## 3.3 Kafka 数据源

### 3.1.1   版本选型

![image-20210808203545664](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808203545664.png)

### 3.1.2  Kafka 0-8 Receiver 模式（当前版本不适用）

![image-20210808203626234](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808203626234.png)

![image-20210808203640058](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808203640058.png)

![image-20210808203657339](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808203657339.png)

### 3.1.3  Kafka 0-8 Direct 模式（当前版本不适用）

![image-20210808203810508](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808203810508.png)

![image-20210808203827882](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808203827882.png)

![image-20210808203858717](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808203858717.png)

![image-20210808203930193](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808203930193.png)

![image-20210808203946905](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808203946905.png)

![image-20210808204008385](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808204008385.png)

![image-20210808204026354](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808204026354.png)

### 3.1.4  Kafka 0-10 Direct模式

![image-20210808204137960](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808204137960.png)

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming-kafka-0-10_2.12</artifactId>
    <version>3.0.0</version>
</dependency>
```

代码

```scala
package com.xupt.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_Kafka {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "linux1:9092,linux2:9092,linux3:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "xupt",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )

        val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("xupt"), kafkaPara)
        )
        kafkaDataDS.map(_.value()).print()

        ssc.start()
        ssc.awaitTermination()
    }
}
```

查看Kafka 消费进度

bin/kafka-consumer-groups.sh --describe --bootstrap-server linux1:9092 --group atguigu

# 4 DStream 转换

DStream 上的操作与 RDD 的类似，分为Transformations（转换）和Output Operations（输出）两种，此外转换操作中还有一些比较特殊的原语，如：updateStateByKey()、transform()以及各种Window 相关的原语。

## 4.1 无状态转化操作









## 4.2 有状态转换

### 4.2.1 UpdateStateByKey

UpdateStateByKey 原语用于记录历史记录，有时，我们需要在 DStream 中跨批次维护状态(例如流计算中累加wordcount)。针对这种情况，updateStateByKey()为我们提供了对一个状态变量的访问，用于键值对形式的 DStream。给定一个由(键，事件)对构成的 DStream，并传递一个指定如何根据新的事件更新每个键对应状态的函数，它可以构建出一个新的 DStream，其内部数据为(键，状态) 对。

updateStateByKey() 的结果会是一个新的DStream，其内部的RDD 序列是由每个时间区间对应的(键，状态)对组成的。

updateStateByKey 操作使得我们可以在用新信息进行更新时保持任意的状态。为使用这个功能，需要做下面两步：

1. 定义状态，状态可以是一个任意的数据类型。

2. 定义状态更新函数，用此函数阐明如何使用之前的状态和来自输入流的新值对状态进行更新。

**使用 updateStateByKey 需要对检查点目录进行配置，会使用检查点来保存状态。**

更新版的wordcount

```scala
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
```

![image-20210808212311198](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210808212311198.png)