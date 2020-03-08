package com.scala.spark_core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:10 上午 2020/3/2
 * @Version: 1.0
 * @Modified By:
 * @Description: Action算子 ，会马上计算，不会延迟就算。注意和转换算子进行区分。
 */
/**
 * Action算子会触发runJob执行，底层源码中当前计算逻辑提交作业submitJob
 */
object Spark14_Action {

  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark14_Action")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    var rdd1: RDD[Int] = sc.makeRDD(1 to 10, 2)
    var rdd2: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 3), ("c", 2)))
    var rdd3: RDD[Int] = sc.parallelize(1 to 10)
    var rdd4: RDD[Int] = sc.parallelize(Array(1, 4, 3, 2, 4, 5))

    // reduce作用：通过func函数聚集RDD中的所有元素，先聚合分区内数据，再聚合分区间数据。
    var reduceRdd1: Int = rdd1.reduce(_ + _)
    val reduceRdd2 = rdd2.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    println("reduce:" + reduceRdd1) //聚合操作 ,会现在 分区内聚合，再在分间进行聚合
    println("reduce:" + reduceRdd2) //聚合操作

    // collect作用：在驱动程序中，以数组的形式返回数据集的所有元素。
    println("collect:" + rdd3.collect()) //在驱动程序中，以数组的形式返回数据集中的所有元素。将RDD的内容收到Driver端进行打印

    // count作用：返回RDD中元素的个数
    println("count:" + rdd3.count()) //返回元素的个数

    // first作用：返回RDD中的第一个元素
    println("first:" + rdd3.first()) //取第一个元素
    // take作用：返回一个由RDD的前n个元素组成的数组
    rdd3.take(3).foreach(println) // 取前三个元素，不排序
    // takeOrdered作用：返回该RDD排序后的前n个元素组成的数组
    rdd3.takeOrdered(3).foreach(println) //取第三个元素，不排序
    rdd4.takeOrdered(3).foreach(println) //取排序后 前三个元素

    // aggregate作用：aggregate函数将每个分区里面的元素通过seqOp和初始值进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。
    // 这个函数最终返回的类型不需要和RDD中元素类型一致。
    println("aggregate :" + rdd1.aggregate(0)(_ + _, _ + _)) //将该RDD所有元素相加得到结果 55,把，先分区内相加，在分区间相加
    println("aggregate 10:" + rdd1.aggregate(10)(_ + _, _ + _)) //将该RDD所有元素相加得到结果85 ，但是初始值会同时作用在分区内核分区间，所以会多10 出来 和 aggregateByKey 不一样的。
    // fold作用：折叠操作，aggregate的简化操作，seqop和combop一样。
    println("fold:" + rdd1.fold(10)(_ + _)) //将所有元素相加得到结果

    /**
     * saveAsTextFile     作用：将数据集的元素以textfile的形式保存到HDFS文件系统或者其他支持的文件系统，对于每个元素，Spark将会调用toString方法，将它装换为文件中的文本
     * saveAsSequenceFile 作用：将数据集中的元素以Hadoop sequencefile的格式保存到指定的目录下，可以使HDFS或者其他Hadoop支持的文件系统。
     * saveAsObjectFile   作用：用于将RDD中的元素序列化成对象，存储到文件中。
     */
    var rdd5: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 3), ("c", 2)))
    rdd5.saveAsTextFile("output1")
    rdd5.saveAsSequenceFile("output2") //#将数据集中的元素以Hadoop sequencefile的格式保存到指定的目录下，可以使HDFS或者其他Hadoop支持的文件系统。
    rdd5.saveAsObjectFile("output3") //byteWrite


    // countByKey作用：针对(K,V)类型的RDD，返回一个(K,Int)的map，表示每一个key对应的元素个数。
    val rdd6 = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)
    println("rdd.countByKey():" + rdd6.countByKey())

    // foreach 作用：在数据集的每一个元素上，运行函数func进行更新。
    var rdd7 = sc.makeRDD(1 to 5, 2)
    rdd7.foreach("foreach:" + println(_))

    sc.stop()
  }
}
