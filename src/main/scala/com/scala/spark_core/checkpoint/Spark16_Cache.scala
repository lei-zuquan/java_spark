package com.scala.spark_core.checkpoint

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:26 下午 2020/3/2
 * @Version: 1.0
 * @Modified By:
 * @Description: cache
 */

/**
 * RDD通过persist方法或cache方法可以将前面的计算结果缓存， 默认情况下 persist() 会把数据以序列化的形式缓存在 JVM的堆空间中。
 * 但是并不是这两个方法被调用时立即缓存，而是触发后面的action时，该RDD将会被缓存在计算节点的内存中，并供后面重用。
 * *
 * 通过查看源码发现cache最终也是调用了persist方法， 默认的存储级别都是仅在内存存储一份，Spark的存储级别还有好多种，
 * 存储级别在object StorageLevel中定义的 。
 *
 * NONE
 * DISK_ONLY
 * DISK_ONLY_2
 * MEMORY_ONLY
 * MEMORY_ONLY_2
 * MEMORY_ONLY_SER
 * MEMORY_ONLY_SER_2
 * MEMORY_AND_DISK
 * MEMORY_AND_DISK_2
 * MEMORY_AND_DISK_SER
 * MEMORY_AND_DISK_SER_2
 * OFF_HEAP
 *
 * 在存储级别的末尾加上 “ _2 ” 来把持久化数据存为两份
 * *
 * 缓存有可能丢失，或者存储存储于内存的数据由于内存不足而被删除，RDD的缓存容错机制保证了即使缓存丢失也能保证计算的正确执行。
 * 通过基于RDD的一系列转换，丢失的数据会被重算，由于RDD的各个Partition是相对独立的，因此只需要计算丢失的部分即可，并不需要重算全部Partition。
 */

object Spark16_Cache {

  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cache")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)
    //（1）创建一个RDD
    val rdd = sc.makeRDD(Array("hello spark: "))
    //（2）将RDD转换为携带当前时间戳不做缓存
    val nocache = rdd.map(_.toString + System.currentTimeMillis)
    //（3）多次打印结果
    nocache.collect.foreach(println)
    nocache.collect.foreach(println)
    nocache.collect.foreach(println)
    nocache.collect.foreach(println)
    nocache.toDebugString

    println("-----------上述是nocache，下述是cache----------")
    //（4）将RDD转换为携带当前时间戳【并】做缓存
    val cache = rdd.map(_.toString + System.currentTimeMillis).cache

    //（5）多次打印做了缓存的结果
    cache.collect.foreach(println)
    cache.collect.foreach(println)
    cache.collect.foreach(println)
    cache.collect.foreach(println)

    cache.toDebugString
  }
}
