# 1 SparkSQL概述



## 1.4 DataFrame是什么

​		在Spark中，DataFrame是一种以RDD为基础的分布式数据集，**类似于传统数据库中的二维表格**。DataFrame与RDD的主要区别在于，前者带有schema元信息，即DataFrame所表示的二维表数据集的每一列都带有名称和类型。这使得Spark SQL得以洞察更多的结构信息，从而对藏于DataFrame背后的数据源以及作用于DataFrame之上的变换进行了针对性的优化，最终达到大幅提升运行时效率的目标。反观RDD，由于无从得知所存数据元素的具体内部结构，Spark Core只能在stage层面进行简单、通用的流水线优化。

​		同时，与Hive类似，DataFrame也支持嵌套数据类型（struct、array和map）。从 API 易用性的角度上看，DataFrame API提供的是一套高层的关系操作，比函数式的RDD API 要更加友好，门槛更低。

![image-20210718185508994](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718185508994.png)

​		上图直观地体现了DataFrame和RDD的区别。
​		左侧的RDD[Person]虽然以Person为类型参数，但Spark框架本身不了解Person类的内部结构。而右侧的DataFrame却提供了详细的结构信息，使得 Spark SQL 可以清楚地知道该数据集中包含哪些列，每列的名称和类型各是什么。
​		DataFrame是为数据提供了Schema的视图。可以把它当做数据库中的一张表来对待
DataFrame也是懒执行的，但性能上比RDD要高，主要原因：优化的执行计划，即查询计划通过Spark catalyst optimiser进行优化。比如下面一个例子:

![image-20210718185551079](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718185551079.png)

​		为了说明查询优化，我们来看上图展示的人口数据分析的示例。图中构造了两个DataFrame，将它们join之后又做了一次filter操作。如果原封不动地执行这个执行计划，最终的执行效率是不高的。因为join是一个代价较大的操作，也可能会产生一个较大的数据集。如果我们能将filter下推到 join下方，先对DataFrame进行过滤，再join过滤后的较小的结果集，便可以有效缩短执行时间。而Spark SQL的查询优化器正是这样做的。简而言之，逻辑查询计划优化就是一个利用基于关系代数的等价变换，将高成本的操作替换为低成本操作的过程。

![image-20210718185630223](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718185630223.png)

## 1.5 DataSet是什么？

​		DataSet是分布式数据集合。DataSet是Spark 1.6中添加的一个新抽象，是DataFrame的一个扩展。它提供了RDD的优势（强类型，使用强大的lambda函数的能力）以及Spark SQL优化执行引擎的优点。DataSet也可以使用功能性的转换（操作map，flatMap，filter等等）。

➢ DataSet是DataFrame API的一个扩展，是SparkSQL最新的数据抽象
➢ 用户友好的API风格，既具有类型安全检查也具有DataFrame的查询优化特性；
➢ 用样例类来对DataSet中定义数据的结构信息，样例类中每个属性的名称直接映射到DataSet中的字段名称；
➢ DataSet是强类型的。比如可以有DataSet[Car]，DataSet[Person]。
➢ DataFrame是DataSet的特列，DataFrame=DataSet[Row] ，所以可以通过as方法将DataFrame转换为DataSet。Row是一个类型，跟Car、Person这些的类型一样，所有的表结构信息都用Row来表示。获取数据时需要指定顺序

# 2 Spark-SQL核心编程

## 2.1 新的起点

​		Spark Core中，如果想要执行应用程序，需要首先构建上下文环境对象SparkContext，Spark SQL其实可以理解为对Spark Core的一种封装，不仅仅在模型上进行了封装，上下文环境对象也进行了封装。
​		在老的版本中，SparkSQL提供两种SQL查询起始点：一个叫SQLContext，用于Spark自己提供的SQL查询；一个叫HiveContext，用于连接Hive的查询。
​		**SparkSession是Spark最新的SQL查询起始点**，实质上是SQLContext和HiveContext的组合，所以在SQLContex和HiveContext上可用的API在SparkSession上同样是可以使用的。SparkSession内部封装了SparkContext，所以计算实际上是由sparkContext完成的。当我们使用 spark-shell 的时候, spark框架会自动的创建一个名称叫做spark的SparkSession对象, 就像我们以前可以自动获取到一个sc来表示SparkContext对象一样

![image-20210718192307885](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718192307885.png)

## 2.2 DataFrame

Spark SQL的DataFrame API 允许我们使用 DataFrame 而不用必须去注册临时表或者生成 SQL 表达式。DataFrame API 既有 transformation操作也有action操作。

### 2.2.1 创建DataFrame

在Spark SQL中SparkSession是创建DataFrame和执行SQL的入口，创建DataFrame有三种方式：通过Spark的数据源进行创建；从一个存在的RDD进行转换；还可以从Hive Table进行查询返回。

1) 从Spark数据源进行创建

- 查看Spark支持创建文件的数据源格式

![image-20210718192558089](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718192558089.png)



- 在spark的bin/data目录中创建user.json文件

```json
{"username":"li","age":20}
```

- 读取json文件创建DataFrame

![image-20210718193358069](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718193358069.png)

注意：如果从内存中获取数据，spark可以知道数据类型具体是什么。如果是数字，默认作为Int处理；但是从文件中读取的数字，不能确定是什么类型，所以用bigint接收，可以和Long类型转换，但是和Int不能进行转换

2) 从RDD进行转换

在后续章节中讨论

3) 从Hive Table进行查询返回

在后续章节中讨论

### 2.2.2 SQL语法

SQL 语法风格是指我们查询数据的时候使用 SQL 语句来查询，这种风格的查询必须要有临时视图或者全局视图来辅助

![image-20210718194401767](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718194401767.png)

![image-20210718194426263](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718194426263.png)

### 2.2.3 DSL语法

DataFrame提供一个特定领域语言(domain-specific language, DSL)去管理结构化的数据。可以在 Scala, Java, Python 和 R 中使用 DSL，使用 DSL 语法风格不必去创建临时视图了

![image-20210718195601938](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718195601938.png)

![image-20210718195805745](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718195805745.png)

### 2.2.4 RDD转DataFrame

在IDEA中开发程序时，如果需要RDD与DF或者DS之间互相操作，那么需要引入 `import spark.implicits._`
这里的spark不是Scala中的包名，而是创建的sparkSession对象的变量名称，所以必须先创建SparkSession对象再导入。这里的spark对象不能使用var声明，因为Scala只支持val修饰的对象的引入。
spark-shell中无需导入，自动完成此操作。

![image-20210718220254674](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718220254674.png)

**实际开发中，一般通过样例类将RDD转换为DataFrame**

![image-20210718220326509](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718220326509.png)

![image-20210718220344025](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718220344025.png)

### 2.2.5 DataFrame转RDD

DataFrame其实就是对RDD的封装，所以可以直接获取内部的RDD

![image-20210718220435500](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718220435500.png)

**注意：此时得到的RDD存储类型为Row**

![image-20210718220536018](https://gitee.com/wnboy/pic_bed/raw/master/img/image-20210718220536018.png)