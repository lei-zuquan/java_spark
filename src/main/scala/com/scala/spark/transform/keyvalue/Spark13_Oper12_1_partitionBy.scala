package com.scala.spark.transform.keyvalue

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 2:41 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: partitionBy算子 作用：对pairRDD进行分区操作，如果原有的partionRDD和现有的partionRDD是一致的话就不进行分区， 否则会生成ShuffleRDD，即会产生shuffle过程。
 */
object Spark13_Oper12_partitionBy {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    // map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。

    // 从指定
    //    var listRDD: RDD[Int] = sc.makeRDD( 1 to 16) //这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数
    var listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("c", 2), ("d", 4)))
    var listRDD2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 6), ("c", 8), ("c", 0), ("d", 22)))
    // 自定义分区
    // var partRDD: RDD[(String, Int)] = listRDD.partitionBy(new MyPartitioner(3))

    // 一般是为了传入一个分区器 new org.apache.spark.HashParititon(2)
    var partRDD: RDD[(String, Int)] = listRDD.partitionBy(new org.apache.spark.HashPartitioner(2))

    partRDD.saveAsTextFile("output")
  }
}

//声明分区器
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = {
    1
  }
}

