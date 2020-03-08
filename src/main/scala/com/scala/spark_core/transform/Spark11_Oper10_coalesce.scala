package com.scala.spark_core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 1:34 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: coalesce算子 作用：缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。
 */
object Spark11_Oper10_coalesce {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("coalesce")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    // map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。
    // 从指定
    var listRDD: RDD[Int] = sc.makeRDD(1 to 16)
    println("缩减分区前 = " + listRDD.partitions.size)

    // 缩减分区，可以简单的理解为合并分区，没有shuffle，会把剩余的数据存储在最后
    var coalesceRDD: RDD[Int] = listRDD.coalesce(3)

    println("缩减分区后 = " + coalesceRDD.partitions.size)

  }
}
