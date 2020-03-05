package com.scala.spark.partitioner

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:12 下午 2020/3/2
 * @Version: 1.0
 * @Modified By:
 * @Description: 使用Hash分区的实操
 */

/**
 * HashPartitioner分区的原理：对于给定的key，计算其hashCode，并除以分区的个数取余，
 * 如果余数小于0，则用余数+分区的个数（否则加0），最后返回的值就是这个key所属的分区ID。
 */
object Spark17_HashPartitioner {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HashPartitioner")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    val nopar = sc.parallelize(List((1, 3), (1, 2), (2, 4), (2, 3), (3, 6), (3, 8)), 8)

    nopar.mapPartitionsWithIndex((index, iter) => {
      Iterator(index.toString + " : " + iter.mkString("|"))
    }).collect.foreach(println)

    println("----------------------------------------")
    val hashpar = nopar.partitionBy(new org.apache.spark.HashPartitioner(7))

    println("hashpar.count:" + hashpar.count)

    println("hashpar.partitioner.toString:" + hashpar.partitioner.toString)

    hashpar.mapPartitions(iter => Iterator(iter.length)).collect().foreach(println)

  }

}
