













# 1 Spark概述

## 1.1Spark是什么

Spark是一种基于内存的快速、通用、可扩展的大数据分析计算引擎。

## 1.2 Hadoop and Spark

在之前的学习中，Hadoop的MapReduce是大家广为熟知的计算框架，那为什么咱们还要学习新的计算框架Spark呢，这里就不得不提到Spark和Hadoop的关系。

首先从时间节点上来看:
➢ Hadoop
⚫ 2006年1月，Doug Cutting加入Yahoo，领导Hadoop的开发
⚫ 2008年1月，Hadoop成为Apache顶级项目
⚫ 2011年1.0正式发布
⚫ 2012年3月稳定版发布
⚫ 2013年10月发布2.X (Yarn)版本

➢ Spark
⚫ 2009年，Spark诞生于伯克利大学的AMPLab实验室
⚫ 2010年，伯克利大学正式开源了Spark项目
⚫ 2013年6月，Spark成为了Apache基金会下的项目
⚫ 2014年2月，Spark以飞快的速度成为了Apache的顶级项目
⚫ 2015年至今，Spark变得愈发火爆，大量的国内公司开始重点部署或者使用Spark

然后我们再从功能上来看:
➢ Hadoop

⚫ Hadoop是由java语言编写的，在分布式服务器集群上存储海量数据并运行分布式分析应用的开源框架
⚫ 作为Hadoop分布式文件系统，HDFS处于Hadoop生态圈的最下层，存储着所有的数据，支持着Hadoop的所有服务。它的理论基础源于Google的TheGoogleFileSystem这篇论文，它是GFS的开源实现。
⚫ MapReduce是一种编程模型，Hadoop根据Google的MapReduce论文将其实现，作为Hadoop的分布式计算模型，是Hadoop的核心。基于这个框架，分布式并行程序的编写变得异常简单。综合了HDFS的分布式存储和MapReduce的分布式计算，Hadoop在处理海量数据时，性能横向扩展变得非常容易。
⚫ HBase是对Google的Bigtable的开源实现，但又和Bigtable存在许多不同之处。HBase是一个基于HDFS的分布式数据库，擅长实时地随机读/写超大规模数据集。它也是Hadoop非常重要的组件。

➢ Spark
⚫ Spark是一种由Scala语言开发的快速、通用、可扩展的大数据分析引擎
⚫ Spark Core中提供了Spark最基础与最核心的功能
⚫ Spark SQL是Spark用来操作结构化数据的组件。通过Spark SQL，用户可以使用SQL或者Apache Hive版本的SQL方言（HQL）来查询数据。
⚫ Spark Streaming是Spark平台上针对实时数据进行流式计算的组件，提供了丰富的处理数据流的API。
由上面的信息可以获知，Spark出现的时间相对较晚，并且主要功能主要是用于数据计算，所以其实Spark一直被认为是Hadoop 框架的升级版。

## 1.3 Spark or Hadoop

Hadoop的MR框架和Spark框架都是数据处理框架，那么我们在使用时如何选择呢？
⚫ Hadoop MapReduce由于其设计初衷并不是为了满足循环迭代式数据流处理，因此在多并行运行的数据可复用场景（如：机器学习、图挖掘算法、交互式数据挖掘算法）中存在诸多计算效率等问题。所以Spark应运而生，Spark就是在传统的MapReduce 计算框架的基础上，利用其计算过程的优化，从而大大加快了数据分析、挖掘的运行和读写速度，并将计算单元缩小到更适合并行计算和重复使用的RDD计算模型。

⚫ 机器学习中ALS、凸优化梯度下降等。这些都需要基于数据集或者数据集的衍生数据反复查询反复操作。MR这种模式不太合适，即使多MR串行处理，性能和时间也是一个问题。数据的共享依赖于磁盘。另外一种是交互式数据挖掘，MR显然不擅长。而Spark所基于的scala语言恰恰擅长函数的处理。
⚫ Spark是一个分布式数据快速分析项目。它的核心技术是弹性分布式数据集（Resilient Distributed Datasets），提供了比MapReduce丰富的模型，可以快速在内存中对数据集进行多次迭代，来支持复杂的数据挖掘算法和图形计算算法。
⚫ Spark和Hadoop的根本差异是多个作业之间的数据通信问题 : Spark多个作业之间数据通信是基于内存，而Hadoop是基于磁盘。

⚫ Spark Task的启动时间快。Spark采用fork线程的方式，而Hadoop采用创建新的进程的方式。
⚫ Spark只有在shuffle的时候将数据写入磁盘，而Hadoop中多个MR作业之间的数据交互都要依赖于磁盘交互
⚫ Spark的缓存机制比HDFS的缓存机制高效。
经过上面的比较，我们可以看出在绝大多数的数据计算场景中，Spark确实会比MapReduce更有优势。但是Spark是基于内存的，所以在实际的生产环境中，由于内存的限制，可能会由于内存资源不够导致Job执行失败，此时，MapReduce其实是一个更好的选择，所以Spark并不能完全替代MR。

## 1.4 Spark核心模块

![image-20210822150649646](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210822150649646.png)





➢ Spark Core
Spark Core中提供了Spark最基础与最核心的功能，Spark其他的功能如：Spark SQL，Spark Streaming，GraphX, MLlib都是在Spark Core的基础上进行扩展的
➢ Spark SQL
Spark SQL是Spark用来操作结构化数据的组件。通过Spark SQL，用户可以使用SQL或者Apache Hive版本的SQL方言（HQL）来查询数据。
➢ Spark Streaming
Spark Streaming是Spark平台上针对实时数据进行流式计算的组件，提供了丰富的处理数据流的API。

➢ Spark MLlib
MLlib是Spark提供的一个机器学习算法库。MLlib不仅提供了模型评估、数据导入等额外的功能，还提供了一些更底层的机器学习原语。
➢ Spark GraphX
GraphX是Spark面向图计算提供的框架与算法库。

# 2 quick start

在大数据早期的课程中我们已经学习了MapReduce框架的原理及基本使用，并了解了其底层数据处理的实现方式。接下来，就让咱们走进Spark的世界，了解一下它是如何带领我们完成数据处理的。

# 2.1 创建Maven项目

### 2.1.1 增加Scala插件

Spark由Scala语言开发的，所以本课件接下来的开发所使用的语言也为Scala，咱们当前使用的Spark版本为3.0.0，默认采用的Scala编译版本为2.12，所以后续开发时。我们依然采用这个版本。开发前请保证IDEA开发工具中含有Scala开发插件

![image-20210822151051985](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210822151051985.png)

### 2.1.2 增加依赖关系

修改Maven项目中的POM文件，增加Spark框架的依赖关系。本课件基于Spark3.0版本，使用时请注意对应版本。

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_2.12</artifactId>
    <version>${sparkversion}</version>
</dependency>
```

### 2.1.3 WordCount

- 实现一

```scala
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // 1 建立和Spark的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sparkContext = new SparkContext(sparkConf)
    // 2 执行业务
    //
    val lines: RDD[String] = sparkContext.textFile("datas")
    var words: RDD[String] = lines.flatMap(_.split(" "))
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount=wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    val array: Array[(String, Int)] = wordCount.collect()
    array.foreach(println)

    // 3 关闭连接
    sparkContext.stop()

  }
}
```

- 实现二

```scala
package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount1 {

    def main(args: Array[String]): Unit = {

        // Application
        // Spark框架
        // TODO 建立和Spark框架的连接
        // JDBC : Connection
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        // TODO 执行业务操作

        // 1. 读取文件，获取一行一行的数据
        //    hello world
        val lines: RDD[String] = sc.textFile("datas")

        // 2. 将一行数据进行拆分，形成一个一个的单词（分词）
        //    扁平化：将整体拆分成个体的操作
        //   "hello world" => hello, world, hello, world
        val words: RDD[String] = lines.flatMap(_.split(" "))

        // 3. 将单词进行结构的转换,方便统计
        // word => (word, 1)
        val wordToOne = words.map(word=>(word,1))

        // 4. 将转换后的数据进行分组聚合
        // 相同key的value进行聚合操作
        // (word, 1) => (word, sum)
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

        // 5. 将转换结果采集到控制台打印出来
        val array: Array[(String, Int)] = wordToSum.collect()
        array.foreach(println)

        // TODO 关闭连接
        sc.stop()

    }
}

```

- 实现三

```scala
package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_WordCount {
    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        wordcount91011(sc)

        sc.stop()

    }

    // groupBy
    def wordcount1(sc : SparkContext): Unit = {

        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val group: RDD[(String, Iterable[String])] = words.groupBy(word=>word)
        val wordCount: RDD[(String, Int)] = group.mapValues(iter=>iter.size)
    }

    // groupByKey
    def wordcount2(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
        val wordCount: RDD[(String, Int)] = group.mapValues(iter=>iter.size)
    }

    // reduceByKey
    def wordcount3(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_+_)
    }

    // aggregateByKey
    def wordcount4(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_+_, _+_)
    }

    // foldByKey
    def wordcount5(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_+_)
    }

    // combineByKey
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

    // countByKey
    def wordcount7(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: collection.Map[String, Long] = wordOne.countByKey()
    }

    // countByValue
    def wordcount8(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordCount: collection.Map[String, Long] = words.countByValue()
    }

    // reduce, aggregate, fold
    def wordcount91011(sc : SparkContext): Unit = {
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

```

### 2.1.4 异常处理

如果本机操作系统是Windows，在程序中使用了Hadoop相关的东西，比如写入文件到HDFS，则会遇到如下异常：

![image-20210822151705633](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210822151705633.png)

出现这个问题的原因，并不是程序的错误，而是windows系统用到了hadoop相关的服务，解决办法是通过配置关联到windows的系统依赖就可以了

![image-20210822152308374](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210822152308374.png)

![image-20210822152232703](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210822152232703.png)

# 3 Spark运行环境

Spark作为一个数据处理框架和计算引擎，被设计在所有常见的集群环境中运行, 在国内工作中主流的环境为Yarn，不过逐渐容器式环境也慢慢流行起来。接下来，我们就分别看看不同环境下Spark的运行

![image-20210822152542578](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210822152542578.png)

## 3.1 Local模式

所谓的Local模式，就是不需要其他任何节点资源就可以在本地执行Spark代码的环境，一般用于教学，调试，演示等，之前在IDEA中运行代码的环境我们称之为开发环境，不太一样。

## 3.2 Standalone模式

local本地模式毕竟只是用来进行练习演示的，真实工作中还是要将应用提交到对应的集群中去执行，这里我们来看看只使用Spark自身节点运行的集群模式，也就是我们所谓的独立部署（Standalone）模式。Spark的Standalone模式体现了经典的master-slave模式。

## 3.3 Yarn模式

独立部署（Standalone）模式由Spark自身提供计算资源，无需其他框架提供资源。这种方式降低了和其他第三方资源框架的耦合性，独立性非常强。但是你也要记住，Spark主要是计算框架，而不是资源调度框架，所以本身提供的资源调度并不是它的强项，所以还是和其他专业的资源调度框架集成会更靠谱一些。所以接下来我们来学习在强大的Yarn环境下Spark是如何工作的（其实是因为在国内工作中，Yarn使用的非常多）。

## 3.4 K8s&Mesos模式

Mesos是Apache下的开源分布式资源管理框架，它被称为是分布式系统的内核,在Twitter得到广泛使用,管理着Twitter超过30,0000台服务器上的应用部署，但是在国内，依然使用着传统的Hadoop大数据框架，所以国内使用Mesos框架的并不多，但是原理其实都差不多，这里我们就不做过多讲解了。

容器化部署是目前业界很流行的一项技术，基于Docker镜像运行能够让用户更加方便地对应用进行管理和运维。容器管理工具中最为流行的就是Kubernetes（k8s），而Spark也在最近的版本中支持了k8s部署模式。这里我们也不做过多的讲解。

## 3.5 Windows模式

在同学们自己学习时，每次都需要启动虚拟机，启动集群，这是一个比较繁琐的过程，并且会占大量的系统资源，导致系统执行变慢，不仅仅影响学习效果，也影响学习进度，Spark非常暖心地提供了可以在windows系统下启动本地集群的方式，这样，在不使用虚拟机的情况下，也能学习Spark的基本使用

### 3.5.1 解压缩文件

将文件spark-3.0.0-bin-hadoop3.2.tgz解压缩到无中文无空格的路径中

执行解压缩文件路径下bin目录中的spark-shell.cmd文件，启动Spark本地环境

# 4 Spark运行架构

## 4.1 运行架构

Spark框架的核心是一个计算引擎，整体来说，它采用了标准 master-slave 的结构。
如下图所示，它展示了一个 Spark执行时的基本结构。图形中的Driver表示master，负责管理整个集群中的作业任务调度。图形中的Executor 则是 slave，负责实际执行任务。

![image-20210822160114906](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210822160114906.png)

## 4.2 核心组件

由上图可以看出，对于Spark框架有两个核心组件：

### 4.2.1 Driver

Spark驱动器节点，用于执行Spark任务中的main方法，负责实际代码的执行工作。Driver在Spark作业执行时主要负责：
➢ 将用户程序转化为作业（job）
➢ 在Executor之间调度任务(task)
➢ 跟踪Executor的执行情况
➢ 通过UI展示查询运行情况
实际上，我们无法准确地描述Driver的定义，因为在整个的编程过程中没有看到任何有关Driver的字眼。所以简单理解，所谓的Driver就是驱使整个应用运行起来的程序，也称之为Driver类。

### 4.2.2 Executor

Spark Executor是集群中工作节点（Worker）中的一个JVM进程，负责在 Spark 作业中运行具体任务（Task），任务彼此之间相互独立。Spark 应用启动时，Executor节点被同时启动，并且始终伴随着整个 Spark 应用的生命周期而存在。如果有Executor节点发生了故障或崩溃，Spark 应用也可以继续执行，会将出错节点上的任务调度到其他Executor节点上继续运行。

Executor有两个核心功能：

➢ 负责运行组成Spark应用的任务，并将结果返回给驱动器进程
➢ 它们通过自身的块管理器（Block Manager）为用户程序中要求缓存的 RDD 提供内存式存储。RDD 是直接缓存在Executor进程内的，因此任务可以在运行时充分利用缓存数据加速运算。

### 4.2.3 Master & Worker

Spark集群的独立部署环境中，不需要依赖其他的资源调度框架，自身就实现了资源调度的功能，所以环境中还有其他两个核心组件：Master和Worker，这里的Master是一个进程，主要负责资源的调度和分配，并进行集群的监控等职责，类似于Yarn环境中的RM, 而Worker呢，也是进程，一个Worker运行在集群中的一台服务器上，由Master分配资源对数据进行并行的处理和计算，类似于Yarn环境中NM。

### 4.2.4 ApplicationMaster

Hadoop用户向YARN集群提交应用程序时,提交程序中应该包含ApplicationMaster，用于向资源调度器申请执行任务的资源容器Container，运行用户自己的程序任务job，监控整个任务的执行，跟踪整个任务的状态，处理任务失败等异常情况。
说的简单点就是，ResourceManager（资源）和Driver（计算）之间的解耦合靠的就是ApplicationMaster。

## 4.3 核心概念

### 4.3.1 Executor or Core

Spark Executor是集群中运行在工作节点（Worker）中的一个JVM进程，是整个集群中的专门用于计算的节点。在提交应用中，可以提供参数指定计算节点的个数，以及对应的资源。这里的资源一般指的是工作节点Executor的内存大小和使用的虚拟CPU核（Core）数量。

应用程序相关启动参数如下：

![image-20210822161343317](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210822161343317.png)

### 4.3.2 并行度

在分布式计算框架中一般都是多个任务同时执行，由于任务分布在不同的计算节点进行计算，所以能够真正地实现多任务并行执行，记住，这里是并行，而不是并发。这里我们将整个集群并行执行任务的数量称之为并行度。那么一个作业到底并行度是多少呢？这个取决于框架的默认配置。应用程序也可以在运行过程中动态修改。

### 4.3.3 有向无环图

![image-20210822161634605](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210822161634605.png)

大数据计算引擎框架我们根据使用方式的不同一般会分为四类，其中第一类就是Hadoop所承载的MapReduce,它将计算分为两个阶段，分别为 Map阶段 和 Reduce阶段。对于上层应用来说，就不得不想方设法去拆分算法，甚至于不得不在上层应用实现多个 Job 的串联，以完成一个完整的算法，例如迭代计算。 由于这样的弊端，催生了支持 DAG 框架的产生。因此，支持 DAG 的框架被划分为第二代计算引擎。如 Tez 以及更上层的 Oozie。这里我们不去细究各种 DAG 实现之间的区别，不过对于当时的 Tez 和 Oozie 来说，大多还是批处理的任务。接下来就是以 Spark 为代表的第三代的计算引擎。第三代计算引擎的特点主要是 Job 内部的 DAG 支持（不跨越 Job），以及实时计算。

这里所谓的有向无环图，并不是真正意义的图形，而是由Spark程序直接映射成的数据流的高级抽象模型。简单理解就是将整个程序计算的执行过程用图形表示出来,这样更直观，更便于理解，可以用于表示程序的拓扑结构。

DAG（Directed Acyclic Graph）有向无环图是由点和线组成的拓扑图形，该图形具有方向，不会闭环。

## 4.4 提交流程

所谓的提交流程，其实就是我们开发人员根据需求写的应用程序通过Spark客户端提交给Spark运行环境执行计算的流程。在不同的部署环境中，这个提交过程基本相同，但是又有细微的区别，我们这里不进行详细的比较，但是因为国内工作中，将Spark引用部署到Yarn环境中会更多一些，所以本课程中的提交流程是基于Yarn环境的。

![image-20210822162104877](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210822162104877.png)

Spark应用程序提交到Yarn环境中执行的时候，一般会有两种部署执行的方式：Client和Cluster。两种模式主要区别在于：Driver程序的运行节点位置。

### 4.2.1 Yarn Client 模式

Client模式将用于监控和调度的Driver模块在客户端执行，而不是在Yarn中，所以一般用于测试。

➢ Driver在任务提交的本地机器上运行
➢ Driver启动后会和ResourceManager通讯申请启动ApplicationMaster
➢ ResourceManager分配container，在合适的NodeManager上启动ApplicationMaster，负责向ResourceManager申请Executor内存
➢ ResourceManager接到ApplicationMaster的资源申请后会分配container，然后ApplicationMaster在资源分配指定的NodeManager上启动Executor进程

➢ Executor进程启动后会向Driver反向注册，Executor全部注册完成后Driver开始执行main函数
➢ 之后执行到Action算子时，触发一个Job，并根据宽依赖开始划分stage，每个stage生成对应的TaskSet，之后将task分发到各个Executor上执行。

### 4.2.2 Yarn Cluster 模式

Cluster模式将用于监控和调度的Driver模块启动在Yarn集群资源中执行。一般应用于实际生产环境。 

➢ 在YARN Cluster模式下，任务提交后会和ResourceManager通讯申请启动ApplicationMaster，
➢ 随后ResourceManager分配container，在合适的NodeManager上启动ApplicationMaster，此时的ApplicationMaster就是Driver。
➢ Driver启动后向ResourceManager申请Executor内存，ResourceManager接到ApplicationMaster的资源申请后会分配container，然后在合适的NodeManager上启动Executor进程
➢ Executor进程启动后会向Driver反向注册，Executor全部注册完成后Driver开始执行main函数，
➢ 之后执行到Action算子时，触发一个Job，并根据宽依赖开始划分stage，每个stage生成对应的TaskSet，之后将task分发到各个Executor上执行。

# 5 Spark核心编程

Spark计算框架为了能够进行高并发和高吞吐的数据处理，封装了三大数据结构，用于处理不同的应用场景。三大数据结构分别是：
➢ RDD : 弹性分布式数据集
➢ 累加器：分布式**共享只写**变量
➢ 广播变量：分布式**共享只读**变量
接下来我们一起看看这三大数据结构是如何在数据处理中使用的。

# 一、RDD

## 1 Rdd 基本概念

### 1.2 什么是RDD

RDD（Resilient Distributed Dataset）叫做弹性分布式数据集，是Spark中最基本的数据处理模型。代码中是一个抽象类，它代表一个弹性的、不可变、可分区、里面的元素可并行计算的集合。

弹性

⚫ 存储的弹性：内存与磁盘的自动切换；
⚫ 容错的弹性：数据丢失可以自动恢复；
⚫ 计算的弹性：计算出错重试机制；
⚫ 分片的弹性：可根据需要重新分片。

分布式：数据存储在大集群不同节点上 
数据集：  RDD封装了计算逻辑，并不保存数据
数据抽象： RDD是一个抽象类，需要子具体实现 
不可变 ：RDD封装了计算逻辑，是不可以改变的， 想改变只能产生新的RDD，在新的RDD里面封装计算逻辑。
可分区、并行计算

 - RDD是最小的计算单元
 - 首选位置：数据发送到哪个节点效率最优 
 - 移动数据不如移动计算

### 1.3 核心属性

```
* Internally, each RDD is characterized by five main properties:
*
*  - A list of partitions
*  - A function for computing each split
*  - A list of dependencies on other RDDs
*  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
*  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
*    an HDFS file)
```

#### 1.3.1 分区列表

RDD 数据结构中存在分区列表，用于执行任务时并行计算，是实现分布式计算的重要属性。

```scala
/**
 * Implemented by subclasses to return the set of partitions in this RDD. This method will only
 * be called once, so it is safe to implement a time-consuming computation in it.
 *
 * The partitions in this array must satisfy the following property:
 *   `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
 */
protected def getPartitions: Array[Partition]
```

#### 1.3.2 分区计算函数

Spark 在计算时，是使用分区函数对每一个分区进行计算

```scala
/**
 * :: DeveloperApi ::
 * Implemented by subclasses to compute a given partition.
 */
@DeveloperApi
def compute(split: Partition, context: TaskContext): Iterator[T]
```

#### 1.3.3 RDD 之间的依赖关系

RDD 是计算模型的封装，当需求中需要将多个计算模型进行组合时，就需要将多个 RDD 建立依赖关系

```scala
/**
 * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
 * be called once, so it is safe to implement a time-consuming computation in it.
 */
protected def getDependencies: Seq[Dependency[_]] = deps
```

#### 1.3.4 分区器（可选）

当数据为 KV 类型数据时，可以通过设定分区器自定义数据的分区

```scala
/** Optionally overridden by subclasses to specify how they are partitioned. */
@transient val partitioner: Option[Partitioner] = None
```

#### 1.3.5 首选位置（可选）

计算数据时，可以根据计算节点的状态选择不同的节点位置进行计算

```scala
/**
 * Optionally overridden by subclasses to specify placement preferences.
 */
protected def getPreferredLocations(split: Partition): Seq[String] = Nil
```

### 1.4 执行原理

从计算的角度来讲，数据处理过程中需要计算资源（内存 & CPU）和计算模型（逻辑）。执行时，需要将计算资源和计算模型进行协调和整合。

Spark 框架在执行时，先申请资源，然后将应用程序的数据处理逻辑分解成一个一个的计算任务。然后将任务发到已经分配资源的计算节点上, 按照指定的计算模型进行数据计算。最后得到计算结果。

RDD 是 Spark 框架中用于数据处理的核心模型，接下来我们看看，在 Yarn 环境中，RDD 的工作原理:

1)    启动 Yarn 集群环境

![image-20210718174037914](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718174037914.png)

2)    Spark 通过申请资源创建调度节点和计算节点

![image-20210718174108061](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718174108061.png)

3)    Spark 框架根据需求将计算逻辑根据分区划分成不同的任务

![image-20210718174131011](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718174131011.png)

4)    调度节点将任务根据计算节点状态发送到对应的计算节点进行计算

![image-20210718174201429](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718174201429.png)

从以上流程可以看出 RDD 在整个流程中主要用于将逻辑进行封装，并生成 Task 发送给Executor 节点执行计算，接下来我们就一起看看 Spark 框架中RDD 是具体是如何进行数据处理的。

### 1.5 基础编程

#### 1.5.1 RDD创建

在 Spark 中创建RDD 的创建方式可以分为四种：

- 从集合（内存）中创建 RDD

从集合中创建RDD，[Spark ](https://www.iteblog.com/archives/tag/spark/)主要提供了两个方法：parallelize 和 makeRDD

从底层代码实现来讲，makeRDD 方法其实就是parallelize 方法

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife 
  */
object Spark01_Rdd_Memory {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 创建RDD
    val seq: Seq[Int] = Seq(1,2,3,4)

    // 方式1
    val rdd_01: RDD[Int] = sc.parallelize(seq)
    // 方式2 ：底层还是调用方式1，推荐使用
    val rdd_02: RDD[Int] = sc.makeRDD(seq)


    rdd_01.foreach(println)
    println("---------------")
    rdd_02.foreach(println)

    // 关闭资源
    sc.stop()

  }
}
```

- 从外部存储（文件）创建RDD

主要是通过一个RDD 运算完后，再产生新的RDD。详情请参考后续章节

- 直接创建 RDD（new）

使用 new 的方式直接构造RDD，一般由Spark 框架自身使用。

## 2 RDD的并行度与分区

默认情况下，Spark 可以将一个作业切分多个任务后，发送给 Executor 节点并行计算，而能够并行计算的任务数量我们称之为并行度。这个数量可以在构建RDD 时指定。记住，这里的并行执行的任务数量，并不是指的切分任务的数量，不要混淆了

1. 读取内存数据

```scala
makeRDD方法可以传递第二个参数，这个参数表示分区的数量
第二个参数可以不传递的，那么makeRDD方法会使用默认值 ： defaultParallelism（默认并行度
默认的并行度：scheduler.conf.getInt("spark.default.parallelism", totalCores)
spark在默认情况下，从配置对象中获取配置参数：spark.default.parallelism
如果获取不到，那么使用totalCores属性，这个属性取值为当前运行环境的最大可用核数
```

数据如何存放到哪个分区：核心源码

```scala
// Sequences need to be sliced at the same set of index positions for operations
// like RDD.zip() to behave as expected
def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
  (0 until numSlices).iterator.map { i =>
    val start = ((i * length) / numSlices).toInt
    val end = (((i + 1) * length) / numSlices).toInt
    (start, end)
  }
}
```

2. 读取文件系统

   ```scala
   // textFile是按照行读取，读取的是一个字符串
   // wholeTextFiles是按照文件读取，读取的元组第一个是文件的路径，第二个是文件的内容
   val fileRdd: RDD[(String, String)] = sc.wholeTextFiles("datas/*")
   ```

文件系统如何分区？

- 文件系统分区数的计算
  1. 计算每个分区文件的大小 ：goalSize=totalSize/numSplits
     - 文件总的字节大小除以分区数，如果未设定分区数，则取1，如果设定分区数，则取设定的值。
  2. 计算分区数：realnumSplits=totalSize/goalSize
     - 总的文件大小除以每个分区的文件大小
     - 如果数据源为多个文件，那么计算分区时以文件为单位进行分区

```scala
object Spark02_Rdd_File_Par {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc: SparkContext = new SparkContext(sparkConf)

    // textFile可以将文件作为数据处理的数据源，默认也可以设定分区。
    //     minPartitions : 最小分区数量
    //     math.min(defaultParallelism, 2)
    //val rdd = sc.textFile("datas/1.txt")
      
    // 如果不想使用默认的分区数量，可以通过第二个参数指定分区数
    // Spark读取文件，底层其实使用的就是Hadoop的读取方式
    // 分区数量的计算方式：总文件的大小除以每个分区文件的大小，每个分区文件的大小等于文件总大小除以分区数，如果不设定分区数，默认是1。
      //  （源码）long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
      
    //    totalSize = 7 文件总字节大小
    //    goalSize =  7 / 2 = 3（byte）每个分区3字节大小
    //    hadoop读取文件的时候，有个1.1原则，如果剩余的文件大小大于10%，则创建一个新的分区，否则将剩余的数据放到前面的分区中
    //    7 / 3 = 2...1 (1.1) + 1 = 3(分区) 总文件的大小除以每个分区文件的大小为分区数

    val fileRdd: RDD[String] = sc.textFile("datas/6.txt",2)

    fileRdd.saveAsTextFile("output")
    sc.stop()
  }
}
```

文件系统数据如何存放到哪个分区：

```scala
object Spark02_Rdd_File_Par1 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Rdd")
    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO 数据分区的分配
    // 1. 数据以行为单位进行读取
    //    spark读取文件，采用的是hadoop的方式读取，所以 一行一行 读取，和字节数没有关系
    // 2. 数据读取时以 偏移量 为单位,偏移量不会被重复读取
    /*
       1@@   => 012
       2@@   => 345
       3     => 6
     */
    // 3. 数据分区的偏移量范围的计算
    // 0 => [0, 3]  => 12
    // 1 => [3, 6]  => 3
    // 2 => [6, 7]  =>

    // 【1,2】，【3】，【】
    val fileRdd: RDD[String] = sc.textFile("datas/6.txt",2)
    fileRdd.saveAsTextFile("output")
    sc.stop()
  }
}
```

分区数据的分配：案例分析

```scala
object Spark02_Rdd_File_Par2 {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD

    // 14byte / 2 = 7byte
    // 14 / 7 = 2(分区)

    /*
    1234567@@  => 012345678
    89@@       => 9101112
    0          => 13

    [0, 7]   => 1234567
    [7, 14]  => 890

     */

    // 如果数据源为多个文件，那么计算分区时以文件为单位进行分区
    val rdd = sc.textFile("datas/word.txt", 2)

    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()
  }
}
```

## 3 RDD算子

RDD方法分为两类：

- 转换：功能的补充和封装，将旧的RDD保存为新的RDD
  - map、flatmap
- 行动：触发任务的调度和作业的执行
  - collect

## RDD 转换算子

### 3.1 map

将处理的数据逐条进行映射转换，这里的转换可以是类型的转换，也可以是值的转换。

1. rdd的计算一个分区内的数据是一个一个执行逻辑

   只有前面一个数据全部的逻辑执行完毕后，才会执行下一个数据。

   分区内数据的执行是有序的。

2. 不同分区数据计算是无序的。

```scala
package com.xupt.rdd.operator.transform.map

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * map 算子
  *
  */
object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    val mapRdd: RDD[Int] = rdd.map(_ * 2)
    mapRdd.collect().foreach(println)

    sc.stop()
  }
}
```

```scala
package com.xupt.rdd.operator.transform.map

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * map 算子
  *
  */
object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5),2)
    val mapRdd: RDD[Int] = rdd.map(num=>{
      println(num + ">>>>>>>>>>>>")
      num
    })
    val mapRdd2: RDD[Int] = mapRdd.map(num=>{
      println(num + "==========")
      num
    })

//    mapRdd2.collect().foreach(println)

    sc.stop()
  }
}
```

```scala
package com.xupt.rdd.operator.transform.map

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * map 算子
  * 小功能：从服务器日志数据apache.log中获取用户请求URL资源路径
  */
object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)
    val logRdd: RDD[String] = sc.textFile("datas/apache.log")
    val urlRdd: RDD[String] = logRdd.map(line => {
      val splitRdd: Array[String] = line.split(" ")
      splitRdd(6)
    })
    urlRdd.collect().foreach(println)
    sc.stop()
  }
}
```



### 3.2 mapPartitions

将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处理，哪怕是过滤数据。

mapPartitions : 可以以分区为单位进行数据转换操作

- 但是会将整个分区的数据加载到内存进行引用
- 如果处理完的数据是不会被释放掉，存在对象的引用。
- 在内存较小，数据量较大的场合下，容易出现内存溢出。

```scala
package com.xupt.rdd.operator.transform.mappartitions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * mapPartitions 算子
  *
  */
object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    // mapPartitions : 可以以分区为单位进行数据转换操作
    //                 但是会将整个分区的数据加载到内存进行引用
    //                 如果处理完的数据是不会被释放掉，存在对象的引用。
    //                 在内存较小，数据量较大的场合下，容易出现内存溢出。
    val mapRdd: RDD[Int] = rdd.mapPartitions(itera => {
      println(">>>>>>>>>>>>>")
      itera.map(_ * 2)
    })

    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
```

```scala
package com.xupt.rdd.operator.transform.mappartitions

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  *  mapPartitions 算子
  *  小功能：获取每个数据分区的最大值
  */
object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRdd: RDD[Int] = rdd.mapPartitions(itera => {
      List(itera.max).iterator
    })

    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
```

思考一个问题：map和mapPartitions的区别？

➢ 数据处理角度
Map算子是分区内一个数据一个数据的执行，类似于串行操作。而mapPartitions算子是以分区为单位进行批处理操作。
➢ 功能的角度
Map算子主要目的将数据源中的数据进行转换和改变。但是不会减少或增多数据。MapPartitions算子需要传递一个迭代器，返回一个迭代器，没有要求的元素的个数保持不变，所以可以增加或减少数据

➢ 性能的角度
Map算子因为类似于串行操作，所以性能比较低，而是mapPartitions算子类似于批处理，所以性能较高。但是mapPartitions算子会长时间占用内存，那么这样会导致内存可能不够用，出现内存溢出的错误。所以在内存有限的情况下，不推荐使用。使用map操作。

### 3.3 mapPartitionsWithIndex

将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处理，哪怕是过滤数据，在处理时同时可以获取当前分区索引。

```scala
package com.xupt.rdd.operator.transform.mappartitionswithindex

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * mapPartitionsWithIndex 算子
  * 小功能：获取第二个数据分区的数据
  *
  */
object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRdd: RDD[Int] = rdd.mapPartitionsWithIndex((index, iterator) => {
      if (index == 1) {
        iterator
      } else {
        Nil.iterator
      }
    })

    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
```

```scala
package com.xupt.rdd.operator.transform.mappartitionswithindex

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * mapPartitionsWithIndex 算子
  *
  */
object Spark03_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRdd: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, iterator) => {
      iterator.map(num => {
        (index, num)
      })
    })

    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
```

### 3.4 flatMap

将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射

```scala
package com.xupt.rdd.operator.transform.flatmap

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  *
  */
object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3,4)))
    val mapRdd: RDD[Int] = rdd.flatMap(list=>list)
    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
```

```scala
package com.xupt.rdd.operator.transform.flatmap

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * flatMap 算子
  *
  */
object Spark04_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello world"))
    val mapRdd: RDD[String] = rdd.flatMap(str=>str.split(" "))

    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
```

```scala
package com.xupt.rdd.operator.transform.flatmap

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * flatMap 算子
  * 小功能：将List(List(1,2),3,List(4,5))进行扁平化操作
  */
object Spark04_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)


    val rdd: RDD[Any] = sc.makeRDD(List(List(1,2),3,List(4,5)))
    val mapRdd: RDD[Any] = rdd.flatMap(date => {
      date match {
        case list: List[_] => list
        case num: Int => List(num)
      }
    })

    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
```



### 3.5 glom

将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变

```scala
package com.xupt.rdd.operator.transform.glom

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * glom 算子
  *
  */
object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val arrRdd: RDD[Array[Int]] = rdd.glom()
    arrRdd.collect().foreach(arr=> println(arr.mkString(",")))
    sc.stop()
  }
}
```

```scala
package com.xupt.rdd.operator.transform.glom

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * glom 算子
  *
  * 小功能：计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
  *
  */
object Spark05_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val arrRdd: RDD[Array[Int]] = rdd.glom()
    println(arrRdd.map(arr => arr.max).sum())
    sc.stop()
  }
}
```

### 3.6 groupBy

groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
相同的key值的数据会放置在一个组中

将数据根据指定的规则进行分组, **分区默认不变**，但是数据会被打乱重新组合，我们将这样的操作称之为shuffle。极限情况下，数据可能被分在同一个分区中
一个组的数据在一个分区中，但是并不是说一个分区中只有一个组

```scala
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
```

```scala
package com.xupt.rdd.operator.transform.groupby

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * groupBy 算子
  *
  */
object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello","Spark","Hadoop","HDFS"),2)

    val groupRdd: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

    groupRdd.collect().foreach(println)

    sc.stop()
  }
}
```

```scala
package com.xupt.rdd.operator.transform.groupby

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  * groupBy 算子
  *
  */
object Spark06_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    val mapRdd: RDD[(String, Int)] = rdd.map(line => {
      val lineRdd: Array[String] = line.split(" ")
      val time: String = lineRdd(3)
      val dateFormat1: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val date: Date = dateFormat1.parse(time)
      val dateFormat2: SimpleDateFormat = new SimpleDateFormat("HH")
      val houre: String = dateFormat2.format(date)
      (houre, 1)
    })
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd.groupBy(_._1)

    val resRdd: RDD[(String, Int)] = groupRdd.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }
    resRdd.collect().foreach(println)

    sc.stop()
  }
}
```

### 3.7 filter

将数据根据指定的规则进行筛选过滤，符合规则的数据保留，不符合规则的数据丢弃。
当数据进行筛选过滤后，分区不变，但是分区内的数据可能不均衡，生产环境下，可能会出现数据倾斜。

```scala
package com.xupt.rdd.operator.transform.filter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  *         filter 算子
  *
  */
object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val filterRdd: RDD[Int] = rdd.filter(num => num % 2 != 0)

    filterRdd.collect().foreach(println)

    sc.stop()
  }
}
```

```scala
package com.xupt.rdd.operator.transform.filter

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *
  *         filter 算子
  *
  */
object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)
    val data: RDD[String] = sc.textFile("datas/apache.log")
    val filterRdd: RDD[String] = data.filter(line => {
      val splitRdd: Array[String] = line.split(" ")
      splitRdd(3).startsWith("17/05/2015")
    })

    filterRdd.collect().foreach(println)

    sc.stop()
  }
}
```

### 3.8 sample

根据指定的规则从数据集中抽取数据，可以用于 **处理数据倾斜**

![image-20210822172028655](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210822172028655.png)

 ### 3.9 distinct

将数据集中重复的数据去重

```scala
package com.xupt.rdd.operator.transform.distinct

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *         distinct算子
  */
object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val data: RDD[Int] = sc.makeRDD(List(1,2,3,4,1,2,3,4))
    // 原理：
    // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)

    // (1, null),(2, null),(3, null),(4, null),(1, null),(2, null),(3, null),(4, null)
    // (1, null)(1, null)(1, null)
    // (null, null) => null
    // (1, null) => 1

    val distinctRdd: RDD[Int] = data.distinct()

    distinctRdd.collect().foreach(println)

    sc.stop()
  }
}
```

### 3.10 coalesce

**根据数据量缩减分区**，用于大数据集过滤后，提高小数据集的执行效率。
当spark程序中，存在过多的小任务的时候，可以通过coalesce方法，收缩合并分区，减少分区的个数，减小任务调度成本。

- coalesce方法默认情况下不会将分区的数据打乱重新组合
- 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
- 如果想要让数据均衡，可以进行**shuffle处理**

coalesce算子可以扩大分区的，但是如果不进行shuffle操作，是没有意义，不起作用。

所以如果想要实现扩大分区的效果，需要使用shuffle操作。

spark提供了一个简化的操作：

- 缩减分区：coalesce，如果想要数据均衡，可以采用shuffle
- 扩大分区：repartition, 底层代码调用的就是coalesce，而且肯定采用shuffle

```scala
package com.xupt.rdd.operator.transform.coalesce

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *         distinct算子
  */
object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    // coalesce方法默认情况下不会将分区的数据打乱重新组合
    // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    // 如果想要让数据均衡，可以进行shuffle处理

//    val coalesecRdd: RDD[Int] = rdd.coalesce(2)
    val coalesecRdd: RDD[Int] = rdd.coalesce(2, true)

    coalesecRdd.saveAsTextFile("output")

    sc.stop()
  }
}
```

### 3.11 repartition

该操作内部其实执行的是coalesce操作，参数shuffle的默认值为true。

无论是将分区数多的RDD转换为分区数少的RDD，还是将分区数少的RDD转换为分区数多的RDD，repartition操作都可以完成，因为无论如何都会经shuffle过程。

```scala
package com.xupt.rdd.operator.transform.repartition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *         distinct算子
  */
object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    // coalesce算子可以扩大分区的，但是如果不进行shuffle操作，是没有意义，不起作用。
    // 所以如果想要实现扩大分区的效果，需要使用shuffle操作
    // spark提供了一个简化的操作
    // 缩减分区：coalesce，如果想要数据均衡，可以采用shuffle
    // 扩大分区：repartition, 底层代码调用的就是coalesce，而且肯定采用shuffle

    //    val coalesecRdd: RDD[Int] = rdd.coalesce(3, true)
    val resRdd: RDD[Int] = rdd.repartition(3)
    resRdd.saveAsTextFile("output")

    sc.stop()
  }
}
```

### 3.12 sortBy

 sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式。
 sortBy默认情况下，不会改变分区。但是中间存在shuffle操作。

```scala
package com.xupt.rdd.operator.transform.sortBy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *         distinct算子
  */
object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 6, 2, 4, 3, 5), 2)

    val sortRdd: RDD[Int] = rdd.sortBy(num => num)
    sortRdd.saveAsTextFile("output")

    sc.stop()
  }
}
```

```scala
package com.xupt.rdd.operator.transform.sortBy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife
  *         distinct算子
  */
object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tranform")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)
    val resRdd: RDD[(String, Int)] = rdd.sortBy(t => t._1, false)

    // sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式
    // sortBy默认情况下，不会改变分区。但是中间存在shuffle操作

    resRdd.collect().mkString(",").foreach(println)
    sc.stop()
  }
}
```



### 3.13 双Value类型

- intersection：交集 对源RDD和参数RDD求交集后返回一个新的RDD
- union：并集 对源RDD和参数RDD求并集后返回一个新的RDD
- subtract：差集 以一个RDD元素为主，去除两个RDD中重复元素，将其他元素保留下来。求差集
- zip：拉链 将两个RDD中的元素，以键值对的形式进行合并。其中，键值对中的Key为第1个RDD中的元素，Value为第2个RDD中的相同位置的元素。

```scala
package com.xupt.rdd.operator.transform.towvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - 双Value类型

        // 交集，并集和差集要求两个数据源数据类型保持一致
        // 拉链操作两个数据源的类型可以不一致

        val rdd1 = sc.makeRDD(List(1,2,3,4))
        val rdd2 = sc.makeRDD(List(3,4,5,6))
        val rdd7 = sc.makeRDD(List("3","4","5","6"))

        // 交集 : 【3，4】
        val rdd3: RDD[Int] = rdd1.intersection(rdd2)
        //val rdd8 = rdd1.intersection(rdd7)
        println(rdd3.collect().mkString(","))

        // 并集 : 【1，2，3，4，3，4，5，6】
        val rdd4: RDD[Int] = rdd1.union(rdd2)
        println(rdd4.collect().mkString(","))

        // 差集 : 【1，2】
        val rdd5: RDD[Int] = rdd1.subtract(rdd2)
        println(rdd5.collect().mkString(","))

        // 拉链 : 【1-3，2-4，3-5，4-6】
        val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
        val rdd8 = rdd1.zip(rdd7)
        println(rdd6.collect().mkString(","))

        sc.stop()

    }
}
```

交集，并集和差集要求两个数据源数据类型保持一致。
拉链操作两个数据源的类型可以不一致。

拉链

- 两个数据源要求分区数量要保持一致

  - ```
    Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    ```

- 两个数据源要求分区中数据数量保持一致

  - ```
    Can only zip RDDs with same number of elements in each partition
    ```

```scala
package com.xupt.rdd.operator.transform.towvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform1 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - 双Value类型
        // Can't zip RDDs with unequal numbers of partitions: List(2, 4)
        // 两个数据源要求分区数量要保持一致
        // Can only zip RDDs with same number of elements in each partition
        // 两个数据源要求分区中数据数量保持一致
        val rdd1 = sc.makeRDD(List(1,2,3,4,5,6),2)
        val rdd2 = sc.makeRDD(List(3,4,5,6),2)

        val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
        println(rdd6.collect().mkString(","))

        sc.stop()

    }
}
```

### 3.14 key-value 类型

#### 1 partitionBy

将数据按照指定Partitioner重新进行分区。Spark默认的分区器是HashPartitioner

- 思考一个问题：如果重分区的分区器和当前RDD的分区器一样怎么办？
  - 那就无法实现分区
- 思考一个问题：Spark还有其他分区器吗？
  - 一共有三个分区器
    - HashPartitioner：默认
    - RangePartitioner：一般用于排序
    - PythonPartitioner：privite的，无法从外部使用
- 思考一个问题：如果想按照自己的方法进行数据分区怎么办？
- 思考一个问题：哪那么多问题？

```scala
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
```

#### 2 reduceByKey

 可以将数据按照相同的Key对Value进行聚合

```scala
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
```

#### 3 groupByKey

将数据源的数据根据key对value进行分组

- 将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
- 元组中的第一个元素就是key
- 元组中的第二个元素就是相同key的value的集合

```scala
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
```

**思考一个问题：reduceByKey和groupByKey的区别？**

scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合。

spark中，shullfe操作必须落盘处理，不能中在内存中数据等待，否则会出现内存溢出，shullfe操作的性能非常低。

- **从shuffle的角度**：reduceByKey和groupByKey都存在shuffle的操作，但是reduceByKey可以在shuffle前对分区内相同key的数据进行预聚合（combine）功能，这样会减少落盘的数据量，而groupByKey只是进行分组，不存在数据量减少的问题，reduceByKey性能比较高。
- **从功能的角度**：reduceByKey其实包含分组和聚合的功能。GroupByKey只能分组，不能聚合，所以在分组聚合的场合下，推荐使用reduceByKey，如果仅仅是分组而不需要聚合。那么还是只能使用groupByKey

**reduceByKey**

- 支持分区内聚合功能，可以有效的减少sheffle时落盘的数量，提升性能。
- **分区内和分区间计算规则是 相同的**

#### 4 aggregateByKey

将数据根据**不同的规则**进行分区内计算和分区间计算

aggregateByKey存在函数柯里化，有两个参数列表

- 第一个参数列表,需要传递一个参数，表示为初始值
  - 主要用于当碰见第一个key的时候，和value进行==分区内==计算

- 第二个参数列表需要传递2个参数
  - 第一个参数表示分区内计算规则
  - 第二个参数表示分区间计算规则

aggregateByKey最终的返回数据结果应该和初始值的类型保持一致

```scala
package com.xupt.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform_aggregateByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    /**
      * 分区内求最大值，分区间求和
      */
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)

    // (a,【1,2】), (a, 【3，4】)
    // (a, 2), (a, 4)
    // (a, 6)

    // aggregateByKey存在函数柯里化，有两个参数列表
    // 第一个参数列表,需要传递一个参数，表示为初始值
    //       主要用于当碰见第一个key的时候，和value进行分区内计算
    // 第二个参数列表需要传递2个参数
    //      第一个参数表示分区内计算规则
    //      第二个参数表示分区间计算规则

    val resRdd: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max, _ + _)

    resRdd.collect().foreach(println)

    sc.stop()

  }
}
```

```scala
package com.xupt.rdd.operator.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform_aggregateByKey1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ),2)

    // (a,【1,2】), (a, 【3，4】)
    // (a, 2), (a, 4)
    // (a, 6)

    // aggregateByKey存在函数柯里化，有两个参数列表
    // 第一个参数列表,需要传递一个参数，表示为初始值
    //       主要用于当碰见第一个key的时候，和value进行分区内计算
    // 第二个参数列表需要传递2个参数
    //      第一个参数表示分区内计算规则
    //      第二个参数表示分区间计算规则

    val resRdd: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max, _ + _)
    resRdd.collect().foreach(println)

    // 分区内和分区间计算规则相同
    val resRdd2: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)
    sc.stop()

  }
}
```



小练习：获取相同key的数据的平均值 => (a, 3),(b, 4)

```scala
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
```

#### 5 foldByKey

当分区内计算规则和分区间计算规则相同时，aggregateByKey就可以简化为foldByKey

```scala
object Spark14_RDD_Operator_Transform_foldByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)), 2)

    // 如果聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法
    val resRdd: RDD[(String, Int)] = rdd.foldByKey(0)( _ + _)

    resRdd.collect().foreach(println)

    sc.stop()

  }
}
```

#### 6 combineByKey

最通用的对key-value型rdd进行聚集操作的聚集函数（aggregation function）。类似于aggregate()，combineByKey()允许用户返回值的类型与输入不一致。

```scala
object Spark14_RDD_Operator_Transform_combineByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ),2)

    // combineByKey : 方法需要三个参数
    // 第一个参数表示：将相同key的第一个数据进行结构的转换，实现操作
    // 第二个参数表示：分区内的计算规则
    // 第三个参数表示：分区间的计算规则

    val combinRdd: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => (t._1 + v, t._2 + 1),
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2))

    val resRdd: RDD[(String, Int)] = combinRdd.mapValues {
      case (k, v) => {
        k / v
      }
    }
    resRdd.collect().foreach(println)
    sc.stop()
  }
}
```

总结：

思考一个问题：reduceByKey、foldByKey、aggregateByKey、combineByKey的区别？

- reduceByKey: 相同key的第一个数据不进行任何计算，分区内和分区间计算规则相同
- FoldByKey: 相同key的第一个数据和初始值进行分区内计算，分区内和分区间计算规则相同
- AggregateByKey：相同key的第一个数据和初始值进行分区内计算，分区内和分区间计算规则可以不相同
- CombineByKey:当计算时，发现数据结构不满足要求时，可以让第一个数据转换结构。分区内和分区间计算规则不相同。

```scala
object Spark14_RDD_Operator_Transform_totalByKey {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - (Key - Value类型)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ),2)

    /*
    reduceByKey:

         combineByKeyWithClassTag[V](
             (v: V) => v, // 第一个值不会参与计算
             func, // 分区内计算规则
             func, // 分区间计算规则
             )

    aggregateByKey :

        combineByKeyWithClassTag[U](
            (v: V) => cleanedSeqOp(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
            cleanedSeqOp, // 分区内计算规则
            combOp,       // 分区间计算规则
            )

    foldByKey:

        combineByKeyWithClassTag[V](
            (v: V) => cleanedFunc(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
            cleanedFunc,  // 分区内计算规则
            cleanedFunc,  // 分区间计算规则
            )

    combineByKey :

        combineByKeyWithClassTag(
            createCombiner,  // 相同key的第一条数据进行的处理函数
            mergeValue,      // 表示分区内数据的处理函数
            mergeCombiners,  // 表示分区间数据的处理函数
            )

     */

    rdd.reduceByKey(_+_) // wordcount
    rdd.aggregateByKey(0)(_+_, _+_) // wordcount
    rdd.foldByKey(0)(_+_) // wordcount
    rdd.combineByKey(v=>v,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y) // wordcount

    sc.stop()

  }
}
```

#### 7 join

```scala
object Spark14_RDD_Operator_Transform_join {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // join : 两个不同数据源的数据，相同的key的value会连接在一起，形成元组
    //        如果两个数据源中key没有匹配上，那么数据不会出现在结果中
    //        如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长，会导致性能降低。

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 9), ("b", 22), ("c", 33)))
    val resRdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    
    resRdd.collect().foreach(println)
      
    sc.stop()
  }
}
```

#### 8 leftOuterJoin

```scala
object Spark14_RDD_Operator_Transform_rightOuterJoin {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 9), ("b", 22)))
    rdd1.leftOuterJoin(rdd2).collect().foreach(println)
    sc.stop()

  }
}
```

### 案例实操

1. 数据准备

   agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。

2. 需求描述：

   统计出每一个省份每个广告被点击数量排行的Top3

3. 需求分析

![image-20210711183952646](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711183952646.png)

4. 代码实现

```scala
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
      ite => ite.toList.sortBy(_._2)(Ordering.Int.reverse)
    )

    // 7. 打印到控制台
    resRdd.collect().foreach(println)

    sc.stop()
  }
}
```

## 行动算子

所谓的行动算子，其实就是触发作业(Job)执行的方法，底层代码调用的是环境对象的runJob方法，底层代码中会创建ActiveJob，并提交执行。

```scala
object Spark14_RDD_Operator_Transform_action {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    
    // TODO - 行动算子
    // 所谓的行动算子，其实就是触发作业(Job)执行的方法
    // 底层代码调用的是环境对象的runJob方法
    // 底层代码中会创建ActiveJob，并提交执行。
    
    rdd.collect().foreach(println)
    sc.stop()

  }
}
```

### 1 reduce & collect & count & first & take & takeOrdered

```scala
object Spark2_RDD_Operator_Transform_reduce {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 1.reduce
    val res: Int = rdd.reduce(_+_)
    println(res)

    // 2. collect
    val array: Array[Int] = rdd.collect()
    println(array.mkString(","))

    // 3. count
    val count: Long = rdd.count()
    println(count)

    // 4.first
    val first: Int = rdd.first()
    println(first)

    // 5. take
    val take: Array[Int] = rdd.take(3)
    println(take.mkString(","))

    // takeOrdered : 数据排序后，取N个数据
    val rdd1 = sc.makeRDD(List(4,2,3,1))
    val ints1: Array[Int] = rdd1.takeOrdered(3)
    println(ints1.mkString(","))

    sc.stop()
  }
}
```

### 2 aggregate

分区的数据通过初始值和**分区内**的数据进行聚合，然后再和初始值进行**分区间**的数据聚合

```scala
object Spark3_RDD_Operator_Transform_aggregate {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // aggregateByKey : 初始值只会参与分区内计算
    // aggregate : 初始值会参与分区内计算,并且和参与分区间计算
    val res: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(res)
    sc.stop()
  }
}
```

 ### 3 folder

aggregate的简化版操作

```scala
object Spark4_RDD_Operator_Transform_folder {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val res: Int = rdd.fold(10)(_ + _)
    println(res)
    sc.stop()
  }
}
```

### 4 countByKey

统计每种key的个数

```scala
object Spark5_RDD_Operator_Transform_countbykey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    //    println(intToLong)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 2), ("b", 2), ("b", 2), ("c", 2)))
    val stringToLong: collection.Map[String, Long] = rdd.countByKey()
    println(stringToLong)

    sc.stop()
  }
}
```

### 5 save

将数据保存到不同格式的文件中

![image-20210822174559791](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210822174559791.png)

```scala
object Spark5_RDD_Operator_Transform_save {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output")

    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a",2),("b",3)))

    // saveAsSequenceFile方法要求数据的格式必须为K-V类型
    rdd2.saveAsSequenceFile("output")

    sc.stop()
  }
}
```

### 6 foreach

分布式遍历RDD中的每一个元素，调用指定函数

```scala
object Spark7_RDD_Operator_Transform_Foreach {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // foreach 其实是Driver端内存集合的循环遍历方法
    rdd.collect().foreach(println)

    println("**********")

    // foreach 其实是Executor端内存数据打印

    // 算子 ： Operator（操作）
    //         RDD的方法和Scala集合对象的方法不一样
    //         集合对象的方法都是在同一个节点的内存中完成的。
    //         RDD的方法可以将计算逻辑发送到Executor端（分布式节点）执行
    //         为了区分不同的处理效果，所以将RDD的方法称之为算子。
    //        RDD的方法外部的操作都是在Driver端执行的，而方法内部的逻辑代码是在Executor端执行。

    rdd.foreach(println)

    sc.stop()
  }
}
```

闭包检测：RDD算子中传递的函数是会包含闭包操作，那么就会进行检测功能

```scala
package com.xupt.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark7_RDD_Operator_Transform_Foreach1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val user: User = new User()

    // SparkException: Task not serializable
    // NotSerializableException: com.atguigu.bigdata.spark.core.rdd.operator.action.Spark07_RDD_Operator_Action$User

    // RDD算子中传递的函数是会包含闭包操作，那么就会进行检测功能
    // 闭包检测
    rdd.foreach(num => {
      println(s"age is :${user.age + num}")
    })

    sc.stop()
  }
}

//class User extends Serializable {

// 样例类在编译时，会自动混入序列化特质（实现可序列化接口）
case class User(){
  val age: Int = 10
}
```

## RDD 序列化

### 1 闭包检查

从计算的角度, ==算子以外的代码都是在Driver端执行, 算子里面的代码都是在Executor端执行==。那么在scala的函数式编程中，就会导致**算子内**经常会用到**算子外**的数据，这样就形成了闭包的效果，如果使用的算子外的数据无法序列化，就意味着无法传值给Executor端执行，就会发生错误，所以需要在执行任务计算前，检测闭包内的对象是否可以进行序列化，这个操作我们称之为闭包检测。Scala2.12版本后闭包编译方式发生了改变

### 2 序列化方法和属性

从计算的角度, 算子以外的代码都是在Driver端执行, 算子里面的代码都是在Executor端执行，看如下代码：

```scala
package com.atguigu.bigdata.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

        val search = new Search("h")

        //search.getMatch1(rdd).collect().foreach(println)
        search.getMatch2(rdd).collect().foreach(println)

        sc.stop()
    }
    // 查询对象
    // 类的构造参数其实是类的属性, 构造参数需要进行闭包检测，其实就等同于类进行闭包检测
    class Search(query:String){

        def isMatch(s: String): Boolean = {
            s.contains(this.query)
        }

        // 函数序列化案例
        def getMatch1 (rdd: RDD[String]): RDD[String] = {
            rdd.filter(isMatch)
        }

        // 属性序列化案例
        def getMatch2(rdd: RDD[String]): RDD[String] = {
            val s = query
            rdd.filter(x => x.contains(s))
        }
    }
}
```

### 3 Kryo序列化框架

参考地址: https://github.com/EsotericSoftware/kryo
Java的序列化能够序列化任何的类。但是比较重（字节多），序列化后，对象的提交也比较大。Spark出于性能的考虑，Spark2.0开始支持另外一种Kryo序列化机制。Kryo速度是Serializable的10倍。当RDD在Shuffle数据的时候，简单数据类型、数组和字符串类型已经在Spark内部使用Kryo来序列化。
注意：即使使用Kryo序列化，也要继承Serializable接口。

```scala
package com.xupt.rdd.seria

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Wnlife 
  */
object Spark02_RDD_Seria {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Search]))
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val search = new Search("h")

    search.getMatch1(rdd).collect().foreach(println)
//    search.getMatch2(rdd).collect().foreach(println)

    sc.stop()
  }

  // 查询对象
  //  class Search(query:String) extends Serializable {
  // 类的构造参数其实是类的属性, 构造参数需要进行闭包检测，其实就等同于类进行闭包检测
  class Search(query: String) extends Serializable {

    def isMatch(s: String): Boolean = {
      s.contains(this.query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      val s = query
      rdd.filter(x => x.contains(s))
    }
  }

}

```

## RDD 依赖关系

### 1 RDD 血缘关系

RDD只支持粗粒度转换，即在大量记录上执行的单个操作。将创建RDD的一系列Lineage（血统）记录下来，以便恢复丢失的分区。RDD的Lineage会记录RDD的元数据信息和转换行为，当该RDD的部分分区数据丢失时，它可以根据这些信息来**重新运算**和恢复丢失的数据分区。

ss

![image-20210711215823218](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711215823218.png)

![image-20210711215747701](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711215747701.png)

![image-20210711220021788](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711220021788.png)

代码示例：

```scala
package com.xupt.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Dep {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        val lines: RDD[String] = sc.textFile("datas/word.txt")
        println(lines.toDebugString)
        println("*************************")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.toDebugString)
        println("*************************")
        val wordToOne = words.map(word=>(word,1))
        println(wordToOne.toDebugString)
        println("*************************")
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        println(wordToSum.toDebugString)
        println("*************************")
        val array: Array[(String, Int)] = wordToSum.collect()
        array.foreach(println)

        sc.stop()

    }
}
```

输出：

```scala
(1) datas/word.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_Dep.scala:13 []
 |  datas/word.txt HadoopRDD[0] at textFile at Spark01_RDD_Dep.scala:13 []
*************************
(1) MapPartitionsRDD[2] at flatMap at Spark01_RDD_Dep.scala:16 []
 |  datas/word.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_Dep.scala:13 []
 |  datas/word.txt HadoopRDD[0] at textFile at Spark01_RDD_Dep.scala:13 []
*************************
(1) MapPartitionsRDD[3] at map at Spark01_RDD_Dep.scala:19 []
 |  MapPartitionsRDD[2] at flatMap at Spark01_RDD_Dep.scala:16 []
 |  datas/word.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_Dep.scala:13 []
 |  datas/word.txt HadoopRDD[0] at textFile at Spark01_RDD_Dep.scala:13 []
*************************
(1) ShuffledRDD[4] at reduceByKey at Spark01_RDD_Dep.scala:22 []
 +-(1) MapPartitionsRDD[3] at map at Spark01_RDD_Dep.scala:19 []
    |  MapPartitionsRDD[2] at flatMap at Spark01_RDD_Dep.scala:16 []
    |  datas/word.txt MapPartitionsRDD[1] at textFile at Spark01_RDD_Dep.scala:13 []
    |  datas/word.txt HadoopRDD[0] at textFile at Spark01_RDD_Dep.scala:13 []
*************************
(spark,1)
(Hello,2)
(world,1)
```

### 2 RDD 依赖关系

这里所谓的依赖关系，其实就是两个相邻RDD之间的关系

```scala
package com.xupt.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Dep {

    def main(args: Array[String]): Unit = {

        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        val lines: RDD[String] = sc.textFile("datas/word.txt")
        println(lines.dependencies)
        println("*************************")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        println(words.dependencies)
        println("*************************")
        val wordToOne = words.map(word=>(word,1))
        println(wordToOne.dependencies)
        println("*************************")
        val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
        println(wordToSum.dependencies)
        println("*************************")
        val array: Array[(String, Int)] = wordToSum.collect()
        array.foreach(println)

        sc.stop()

    }
}
```

输出：

```scala
List(org.apache.spark.OneToOneDependency@799ed4e8)
*************************
List(org.apache.spark.OneToOneDependency@712cfb63)
*************************
List(org.apache.spark.OneToOneDependency@a098d76)
*************************
List(org.apache.spark.ShuffleDependency@48df4071)
*************************
(spark,1)
(Hello,2)
(world,1)
```

### 3 窄依赖

窄依赖表示每一个父(上游)RDD的Partition最多被子（下游）RDD的一个Partition使用，窄依赖我们形象的比喻为独生子女。

```scala
class OneToOneDependency[T](rdd : org.apache.spark.rdd.RDD[T]) extends org.apache.spark.NarrowDependency[T]
```

![image-20210711222347625](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711222347625.png)

![image-20210711223004212](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711223004212.png)

### 4 宽依赖

宽依赖表示同一个父（上游）RDD的Partition被多个子（下游）RDD的Partition依赖，会引起Shuffle，总结：宽依赖我们形象的比喻为多生。

```scala
class ShuffleDependency[K, V, C]
```

![image-20210711222541472](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711222541472.png)

![image-20210711223102434](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711223102434.png)

### 5 RDD 阶段划分

DAG（Directed Acyclic Graph）有向无环图是由点和线组成的拓扑图形，该图形具有方向，不会闭环。例如，DAG记录了RDD的转换过程和任务的阶段。

![image-20210711231323008](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711231323008.png)

![image-20210711231413116](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210711231413116.png)

### 6 RDD任务划分

RDD任务切分中间分为：Application、Job、Stage和Task

⚫ Application：初始化一个SparkContext即生成一个Application；
⚫ Job：一个Action算子就会生成一个Job；
⚫ Stage：Stage等于宽依赖(ShuffleDependency)的个数加1；
⚫ Task：一个Stage阶段中，最后一个RDD的分区个数就是Task的个数。

**注意：Application->Job->Stage->Task每一层都是1对n的关系。**

![image-20210712230906296](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210712230906296.png)

![image-20210712224836678](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210712224836678.png)

## RDD持久化

### 1 RDD缓存

RDD通过Cache或者Persist方法将前面的计算结果缓存，默认情况下会把数据以缓存在JVM的堆内存中。但是并不是这两个方法被调用时立即缓存，而是触发后面的action算子时，该RDD将会被缓存在计算节点的内存中，并供后面重用。

缓存有可能丢失，或者存储于内存的数据由于内存不足而被删除，RDD的缓存容错机制保证了即使缓存丢失也能保证计算的正确执行。通过基于RDD的一系列转换，丢失的数据会被重算，由于RDD的各个Partition是相对独立的，因此只需要计算丢失的部分即可，并不需要重算全部Partition。

Spark会自动对一些Shuffle操作的中间数据做持久化操作(比如：reduceByKey)。这样做的目的是为了当一个节点Shuffle失败了避免重新计算整个输入。但是，在实际使用的时候，如果想重用数据，仍然建议调用persist或cache。

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word => {
      println("@@@@@@@@@@@@")
      (word, 1)
    })
    // cache默认持久化的操作，只能将数据保存到内存中，如果想要保存到磁盘文件，需要更改存储级别
    // mapRDD.cache()

    // 持久化操作必须在行动算子执行时完成的。
    mapRDD.persist(StorageLevel.DISK_ONLY)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("**************************************")
    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
```

### 2 RDD CheckPoint检查点

所谓的检查点其实就是通过将RDD中间结果写入磁盘

由于血缘依赖 过长会造成容错本高，这样就不如在中间阶段做检查点容错，如果检测点之后节点出现问题，可以从检测点做血缘，减少了开销。

对RDD做checkpoint操作不会马上执行，必须ActionAction  操作才能触发。

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {

    def main(args: Array[String]): Unit = {
        val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
        val sc = new SparkContext(sparConf)
        // 设置的保存路径
        sc.setCheckpointDir("cp")

        val list = List("Hello Scala", "Hello Spark")

        val rdd = sc.makeRDD(list)

        val flatRDD = rdd.flatMap(_.split(" "))

        val mapRDD = flatRDD.map(word=>{
            println("@@@@@@@@@@@@")
            (word,1)
        })
        // checkpoint 需要落盘，需要指定检查点保存路径
        // 检查点路径保存的文件，当作业执行完毕后，不会被删除
        // 一般保存路径都是在分布式存储系统：HDFS
        // checkpoint会导致从数据源再执行一遍
        mapRDD.checkpoint()

        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        reduceRDD.collect().foreach(println)
        println("**************************************")
        val groupRDD = mapRDD.groupByKey()
        groupRDD.collect().foreach(println)


        sc.stop()
    }
}

```

### 3 缓存和检查点区别

1）Cache缓存只是将数据保存起来，不切断血缘依赖。Checkpoint检查点切断血缘依赖。
2）Cache缓存的数据通常存储在磁盘、内存等地方，可靠性低。Checkpoint的数据通常存储在HDFS等容错、高可用的文件系统，可靠性高。
3）建议对checkpoint()的RDD使用Cache缓存，这样checkpoint的job只需从Cache缓存中读取数据即可，否则需要再从头计算一次RDD。

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Persist {

    def main(args: Array[String]): Unit = {

        // cache : 将数据临时存储在内存中进行数据重用
        //         会在血缘关系中添加新的依赖。一旦，出现问题，可以重头读取数据
        // persist : 将数据临时存储在磁盘文件中进行数据重用
        //           涉及到磁盘IO，性能较低，但是数据安全
        //           如果作业执行完毕，临时保存的数据文件就会丢失
        // checkpoint : 将数据长久地保存在磁盘文件中进行数据重用
        //           涉及到磁盘IO，性能较低，但是数据安全
        //           为了保证数据安全，所以一般情况下，会独立执行作业
        //           为了能够提高效率，一般情况下，是需要和cache联合使用
        //           执行过程中，会切断血缘关系。重新建立新的血缘关系
        //           checkpoint等同于改变数据源

        val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
        val sc = new SparkContext(sparConf)
        sc.setCheckpointDir("cp")

        val list = List("Hello Scala", "Hello Spark")

        val rdd = sc.makeRDD(list)

        val flatRDD = rdd.flatMap(_.split(" "))

        val mapRDD = flatRDD.map(word=>{
            (word,1)
        })
        //mapRDD.cache()
        mapRDD.checkpoint()
        println(mapRDD.toDebugString)
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        reduceRDD.collect().foreach(println)
        println("**************************************")
        println(mapRDD.toDebugString)

        sc.stop()
    }
}
```

## RDD分区器

Spark目前支持Hash分区和Range分区，和用户自定义分区。Hash分区为当前的默认分区。分区器直接决定了RDD中分区的个数、RDD中每条数据经过Shuffle后进入哪个分区，进而决定了Reduce的个数。

- 只有Key-Value类型的RDD才有分区器，非Key-Value类型的RDD分区的值是None
- 每个RDD的分区ID范围：0 ~ (numPartitions - 1)，决定这个值是属于那个分区的。

![image-20210714232509584](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210714232509584.png)

### 1 Hash分区：对于给定的key，计算其hashCode,并除以分区个数取余

```scala
class HashPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
```

### 2 Range分区：将一定范围内的数据映射到一个分区中，尽量保证每个分区数据均匀，而且分区间有序。

```scala
class RangePartitioner[K : Ordering : ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    private var ascending: Boolean = true,
    val samplePointsPerPartitionHint: Int = 20)
  extends Partitioner {

  // A constructor declared in order to maintain backward compatibility for Java, when we add the
  // 4th constructor parameter samplePointsPerPartitionHint. See SPARK-22160.
  // This is added to make sure from a bytecode point of view, there is still a 3-arg ctor.
  def this(partitions: Int, rdd: RDD[_ <: Product2[K, V]], ascending: Boolean) = {
    this(partitions, rdd, ascending, samplePointsPerPartitionHint = 20)
  }

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")
  require(samplePointsPerPartitionHint > 0,
    s"Sample points per partition must be greater than 0 but found $samplePointsPerPartitionHint")

  private var ordering = implicitly[Ordering[K]]

  // An array of upper bounds for the first (partitions - 1) partitions
  private var rangeBounds: Array[K] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      // Cast to double to avoid overflowing ints or longs
      val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.length).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        RangePartitioner.determineBounds(candidates, math.min(partitions, candidates.size))
      }
    }
  }

  def numPartitions: Int = rangeBounds.length + 1

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_, _] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeBoolean(ascending)
        out.writeObject(ordering)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ascending = in.readBoolean()
        ordering = in.readObject().asInstanceOf[Ordering[K]]
        binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}
```

### 3 自定义分区

```scala
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * @author Wnlife 
  */
object Spark_01_RDD_Partitioner {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "123456"), ("cba", "93567"), ("gba", "123456"), ("nba", "123456")
    ), 3)

    val parRdd: RDD[(String, String)] = rdd.partitionBy(new myPartitioner())
    parRdd.saveAsTextFile("output")
  }

  /**
    * 自定义分区器
    * 1. 继承Partitioner
    * 2. 重写方法
    */
  class myPartitioner extends Partitioner {
    // 分区数量
    override def numPartitions: Int = 3
    // 根据数据的key值返回数据所在的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }
    }
  }
}
```

## RDD文件读取与保存

Spark的数据读取及数据保存可以从两个维度来作区分：文件格式以及文件系统。
文件格式分为：text文件、csv文件、sequence文件以及Object文件；
文件系统分为：本地文件系统、HDFS、HBASE以及数据库。

- text文件
- sequence文件
  - SequenceFile文件是Hadoop用来存储二进制形式的key-value对而设计的一种平面文件(Flat File)。在SparkContext中，可以调用sequenceFile[keyClass, valueClass](path)。
- object对象文件
  - 对象文件是将对象序列化后保存的文件，采用Java的序列化机制。可以通过objectFile[T: ClassTag](path)函数接收一个路径，读取对象文件，返回对应的RDD，也可以通过调用saveAsObjectFile()实现对对象文件的输出。因为是序列化所以要指定类型。

保存：

```scala
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Save {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(
      List(
        ("a", 1),
        ("b", 2),
        ("c", 3)
      )
    )

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")

    sc.stop()
  }
}
```

读取：

```scala
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Load {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd = sc.textFile("output1")
    println(rdd.collect().mkString(","))

    val rdd1 = sc.objectFile[(String, Int)]("output2")
    println(rdd1.collect().mkString(","))

    val rdd2 = sc.sequenceFile[String, Int]("output3")
    println(rdd2.collect().mkString(","))

    sc.stop()
  }
}
```

# 二、Spark累加器

## 1 实现原理

累加器用来把Executor端变量信息聚合到Driver端。在Driver程序中定义的变量，在Executor端的每个Task都会得到这个变量的一份新的副本，每个task更新这些副本的值后，传回Driver端进行merge。

![image-20210716195614247](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210716195614247.png)

## 2 系统累加器

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    // 获取系统累加器
    // Spark默认就提供了简单数据聚合的累加器
    // 累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    //    sc.doubleAccumulator
    //sc.collectionAccumulator

    rdd.foreach(v => {
      // 使用累加器
      sumAcc.add(v)
    })
    // 获取累加器的值
    println(sumAcc.value)

    sc.stop()
  }
}
```

累加器的使用注意：一般情况下，累加器会放置在行动算子进行操作

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Acc {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    // 获取系统累加器
    // Spark默认就提供了简单数据聚合的累加器
    // 累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    //    sc.doubleAccumulator
    //sc.collectionAccumulator

    val value: RDD[Int] = rdd.map(v => {
      // 使用累加器
      sumAcc.add(v)
      v
    })

    // 获取累加器的值
    // 少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 多加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 一般情况下，累加器会放置在行动算子进行操作

    value.collect()
    value.collect()

    // 获取累加器的值
    println(sumAcc.value)

    sc.stop()
  }
}
```

## 3 自定义累加器

累加器实现wordcount

```scala
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_Acc_WordCount {
  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List("hello", "spark", "hello"))

    // 累加器 : WordCount
    // 创建累加器对象
    val wcAcc = new MyAccumulator()

    // 向Spark进行注册
    sc.register(wcAcc, "wordCountAcc")

    rdd.foreach(
      word => {
        // 数据的累加（使用累加器）
        wcAcc.add(word)
      }
    )

    // 获取累加器累加的结果
    println(wcAcc.value)

    sc.stop()

  }

  /*
    自定义数据累加器：WordCount

    1. 继承AccumulatorV2, 定义泛型
       IN : 累加器输入的数据类型 String
       OUT : 累加器返回的数据类型 mutable.Map[String, Long]

    2. 重写方法（6）
   */
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    private var wcMap = mutable.Map[String, Long]()

    // 判断是否初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    // 获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCnt = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCnt)
    }

    // Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {

      val map1 = this.wcMap
      val map2 = other.value

      map2.foreach {
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}
```

# 三、广播变量

## 1 实现原理

广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用。比如，如果你的应用需要向所有节点发送一个较大的只读查询表，广播变量用起来都很顺手。在多个并行操作中使用同一个变量，但是 Spark会为每个任务分别发送。

![image-20210716214441584](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210716214441584.png)

## 2 代码变量

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark02_Acc {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    // join会导致数据量几何增长，并且会影响shuffle的性能，不推荐使用
    //    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))
    //    val resRdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    // (a, 1),    (b, 2),    (c, 3)
    // (a, (1,4)),(b, (2,5)),(c, (3,6))
    rdd1.map {
      case (w, c) => {
        val newCount: Int = map.getOrElse(w, 0)
        (w, (c, newCount))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
```

```scala
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark02_Bc {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    // 封装广播变量
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    val mapBr: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    // (a, 1),    (b, 2),    (c, 3)
    // (a, (1,4)),(b, (2,5)),(c, (3,6))
    rdd1.map {
      case (w, c) => {
        // 方法广播变量
        val newCount: Int = mapBr.value.getOrElse(w, 0)
        (w, (c, newCount))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
```