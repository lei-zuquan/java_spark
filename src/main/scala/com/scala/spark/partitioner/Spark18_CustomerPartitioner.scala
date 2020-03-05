package com.scala.spark.partitioner

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:02 下午 2020/3/5
 * @Version: 1.0
 * @Modified By:
 * @Description: CustomerPartitioner 自定义分区器
 */

/**
 * 要实现自定义的分区器，你需要继承 org.apache.spark.Partitioner 类并实现下面三个方法。
 * （1）numPartitions: Int:返回创建出来的分区数。
 * （2）getPartition(key: Any): Int:返回给定键的分区编号(0到numPartitions-1)。
 * （3）equals():Java 判断相等性的标准方法。这个方法的实现非常重要，
 * Spark 需要用这个方法来检查你的分区器对象是否和其他分区器实例相同，这样 Spark 才可以判断两个 RDD 的分区方式是否相同。
 * 需求：将相同后缀的数据写入相同的文件，通过将相同后缀的数据分区到相同的分区并保存输出来实现。
 *
 * 使用自定义的 Partitioner 是很容易的:只要把它传给 partitionBy() 方法即可。
 * Spark 中有许多依赖于数据混洗的方法，比如 join() 和 groupByKey()，它们也可以接收一个可选的 Partitioner 对象来控制输出数据的分区方式。
 */
object Spark18_CustomerPartitioner {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CustomerPartitioner")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    //（1）创建一个pairRDD
    val data = sc.parallelize(Array((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)))

    //（3）将RDD使用自定义的分区类进行重新分区
    val par = data.partitionBy(new Spark18_CustomerPartitioner(2))

    //（4）查看重新分区后的数据分布
    par.mapPartitionsWithIndex((index, items) => items.map((index, _))).collect.foreach(println)
  }

}


// （2）定义一个自定义分区类
class Spark18_CustomerPartitioner(numParts: Int) extends org.apache.spark.Partitioner {

  //覆盖分区数
  override def numPartitions: Int = numParts

  //覆盖分区号获取函数
  override def getPartition(key: Any): Int = {
    val ckey: String = key.toString
    ckey.substring(ckey.length - 1).toInt % numParts
  }
}
