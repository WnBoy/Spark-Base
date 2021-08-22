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

无状态转化操作就是把简单的RDD转化操作应用到每个批次上，也就是转化DStream中的每一个RDD。部分无状态转化操作列在了下表中。注意，针对键值对的DStream转化操作(比如 reduceByKey())要添加import StreamingContext._才能在Scala中使用。

![image-20210815145538564](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210815145538564.png)

需要记住的是，尽管这些函数看起来像作用在整个流上一样，但事实上每个DStream在内部是由许多RDD（批次）组成，且无状态转化操作是分别应用到每个RDD上的。
例如：reduceByKey()会归约每个时间区间中的数据，但不会归约不同区间之间的数据。

### 4.1.1 Transform

Transform允许DStream上执行任意的RDD-to-RDD函数。即使这些函数并没有在DStream的API中暴露出来，通过该函数可以方便的扩展Spark API。该函数每一批次调度一次。其实也就是对DStream中的RDD应用转换。

```scala
package com.xupt.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val lines = ssc.socketTextStream("localhost", 9999)

        // transform方法可以将底层RDD获取到后进行操作
        // 1. DStream功能不完善
        // 2. 需要代码周期性的执行

        // Code : Driver端
        val newDS: DStream[String] = lines.transform(
            rdd => {
                // Code : Driver端，（周期性执行）
                rdd.map(
                    str => {
                        // Code : Executor端
                        str
                    }
                )
            }
        )
        // Code : Driver端
        val newDS1: DStream[String] = lines.map(
            data => {
                // Code : Executor端
                data
            }
        )

        ssc.start()
        ssc.awaitTermination()
    }
}
```

### 4.1.2 Join

![image-20210815152135524](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210815152135524.png)

```scala
package com.xupt.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Join {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        val data9999 = ssc.socketTextStream("localhost", 9999)
        val data8888 = ssc.socketTextStream("localhost", 8888)

        val map9999: DStream[(String, Int)] = data9999.map((_,9))
        val map8888: DStream[(String, Int)] = data8888.map((_,8))

        // 所谓的DStream的Join操作，其实就是两个RDD的join
        val joinDS: DStream[(String, (Int, Int))] = map9999.join(map8888)

        joinDS.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
```

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

### 4.2.2 WindowOperations

Window Operations可以设置窗口的大小和滑动窗口的间隔来动态的获取当前Steaming的允许状态。所有基于窗口的操作都需要两个参数，分别为窗口时长以及滑动步长。

- 窗口时长：计算内容的时间范围；
- 滑动步长：隔多久触发一次计算。

==注意：这两者都必须为采集周期大小的整数倍==



```scala
package com.xupt.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Window {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val lines = ssc.socketTextStream("localhost", 9999)
        val wordToOne = lines.map((_,1))

        // 窗口的范围应该是采集周期的整数倍
        // 窗口可以滑动的，但是默认情况下，一个采集周期进行滑动
        // 这样的话，可能会出现重复数据的计算，为了避免这种情况，可以改变滑动的滑动（步长）
        val windowDS: DStream[(String, Int)] = wordToOne.window(Seconds(6), Seconds(6))

        val wordToCount = windowDS.reduceByKey(_+_)

        wordToCount.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
```

关于Window的操作还有如下方法：
（1）window(windowLength, slideInterval): 基于对源DStream窗化的批次进行计算返回一个新的Dstream；
（2）countByWindow(windowLength, slideInterval): 返回一个滑动窗口计数流中的元素个数；
（3）reduceByWindow(func, windowLength, slideInterval): 通过使用自定义函数整合滑动区间流元素来创建一个新的单元素流；
（4）reduceByKeyAndWindow(func, windowLength, slideInterval, [numTasks]): 当在一个(K,V)对的DStream上调用此函数，会返回一个新(K,V)对的DStream，此处通过对滑动窗口中批次数据使用reduce函数来整合每个key的value值。
（5）reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks]): 这个函数是上述函数的变化版本，每个窗口的reduce值都是通过用前一个窗的reduce值来递增计算。通过reduce进入到滑动窗口数据并”反向reduce”离开窗口的旧数据来实现这个操作。一个例子是随着窗口滑动对keys的“加”“减”计数。通过前边介绍可以想到，这个函数只适用于”可逆的reduce函数”，也就是这些reduce函数有相应的”反reduce”函数(以参数invFunc形式传入)。如前述函数，reduce任务的数量通过可选参数来配置。

```scala
package com.xupt.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Window1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")

        val lines = ssc.socketTextStream("localhost", 9999)
        val wordToOne = lines.map((_,1))

        // reduceByKeyAndWindow : 当窗口范围比较大，但是滑动幅度比较小，那么可以采用增加数据和删除数据的方式
        // 无需重复计算，提升性能。
        val windowDS: DStream[(String, Int)] =
            wordToOne.reduceByKeyAndWindow(
                (x:Int, y:Int) => { x + y},
                (x:Int, y:Int) => {x - y},
                Seconds(9), Seconds(3))

        windowDS.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
```

![image-20210815155128331](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210815155128331.png)

# 5 DStream输出

输出操作指定了对流数据经转化操作得到的数据所要执行的操作(例如把结果推入外部数据库或输出到屏幕上)。与RDD中的惰性求值类似，如果一个DStream及其派生出的DStream都没有被执行输出操作，那么这些DStream就都不会被求值。如果StreamingContext中没有设定输出操作，整个context就都不会启动。
输出操作如下：

➢ print()：在运行流程序的驱动结点上打印DStream中每一批次数据的最开始10个元素。这用于开发和调试。在Python API中，同样的操作叫print()。
➢ saveAsTextFiles(prefix, [suffix])：以text文件形式存储这个DStream的内容。每一批次的存储文件名基于参数中的prefix和suffix。”prefix-Time_IN_MS[.suffix]”。
➢ saveAsObjectFiles(prefix, [suffix])：以Java对象序列化的方式将Stream中的数据保存为 SequenceFiles . 每一批次的存储文件名基于参数中的为"prefix-TIME_IN_MS[.suffix]". Python中目前不可用。
➢ saveAsHadoopFiles(prefix, [suffix])：将Stream中的数据保存为 Hadoop files. 每一批次的存储文件名基于参数中的为"prefix-TIME_IN_MS[.suffix]"。Python API 中目前不可用。
➢ foreachRDD(func)：这是最通用的输出操作，即将函数 func 用于产生于 stream的每一个RDD。其中参数传入的函数func应该实现将每一个RDD中数据推送到外部系统，如将RDD存入文件或者通过网络将其写入数据库。

通用的输出操作foreachRDD()，它用来对DStream中的RDD运行任意计算。这和transform() 有些类似，都可以让我们访问任意RDD。在foreachRDD()中，可以重用我们在Spark中实现的所有行动操作。比如，常见的用例之一是把数据写到诸如MySQL的外部数据库中。
注意：
1) 连接不能写在driver层面（序列化）
2) 如果写在foreach则每个RDD中的每一条数据都创建，得不偿失；
3) 增加foreachPartition，在分区创建（获取）。

```scala
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_Output {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")

        val lines = ssc.socketTextStream("localhost", 9999)
        val wordToOne = lines.map((_,1))

        val windowDS: DStream[(String, Int)] =
            wordToOne.reduceByKeyAndWindow(
                (x:Int, y:Int) => { x + y},
                (x:Int, y:Int) => {x - y},
                Seconds(9), Seconds(3))
        // SparkStreaming如何没有输出操作，那么会提示错误
        //windowDS.print()

        ssc.start()
        ssc.awaitTermination()
    }

}
```

```scala
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_Output1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")

        val lines = ssc.socketTextStream("localhost", 9999)
        val wordToOne = lines.map((_,1))
        
        val windowDS: DStream[(String, Int)] =
            wordToOne.reduceByKeyAndWindow(
                (x:Int, y:Int) => { x + y},
                (x:Int, y:Int) => {x - y},
                Seconds(9), Seconds(3))

        // foreachRDD不会出现时间戳
        windowDS.foreachRDD(
            rdd => {

            }
        )

        ssc.start()
        ssc.awaitTermination()
    }

}
```

# 6 优雅关闭

![image-20210815162128558](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210815162128558.png)

```scala
package com.xupt.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming08_Close {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val lines = ssc.socketTextStream("localhost", 9999)
        val wordToOne = lines.map((_,1))

        wordToOne.print()

        ssc.start()

        // 如果想要关闭采集器，那么需要创建新的线程
        // 而且需要在第三方程序中增加关闭状态
        new Thread(
            new Runnable {
                override def run(): Unit = {
                    // 优雅地关闭
                    // 计算节点不在接收新的数据，而是将现有的数据处理完毕，然后关闭
                    // Mysql : Table(stopSpark) => Row => data
                    // Redis : Data（K-V）
                    // ZK    : /stopSpark
                    // HDFS  : /stopSpark
                    /*
                    while ( true ) {
                        if (true) {
                            // 获取SparkStreaming状态
                            val state: StreamingContextState = ssc.getState()
                            if ( state == StreamingContextState.ACTIVE ) {
                                ssc.stop(true, true)
                            }
                        }
                        Thread.sleep(5000)
                    }
                     */

                    Thread.sleep(5000)
                    val state: StreamingContextState = ssc.getState()
                    if ( state == StreamingContextState.ACTIVE ) {
                        ssc.stop(true, true)
                    }
                    System.exit(0)
                }
            }
        ).start()

        ssc.awaitTermination() // block 阻塞main线程
    }
}
```

从检查点恢复数据

```scala
package com.xupt.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming09_Resume {

  def main(args: Array[String]): Unit = {

    val ssc = StreamingContext.getActiveOrCreate("cp", () => {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      val ssc = new StreamingContext(sparkConf, Seconds(3))
      val lines = ssc.socketTextStream("localhost", 9999)
      val wordToOne = lines.map((_, 1))
      wordToOne.print()
      ssc
    })

    ssc.checkpoint("cp")
    new Thread(
      () => {
        Thread.sleep(5000)
        val state: StreamingContextState = ssc.getState()
        if (state == StreamingContextState.ACTIVE) {
          ssc.stop(true, true)
        }
        System.exit(0)
      }
    ).start()
    ssc.start()
    ssc.awaitTermination() // block 阻塞main线程
  }
}
```





