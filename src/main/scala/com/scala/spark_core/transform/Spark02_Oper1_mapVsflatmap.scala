package com.scala.spark_core.transform

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:45 上午 2020/8/19
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
object Spark02_Oper1_mapVsflatmap {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("map")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    // map算子
    var listRDD: RDD[String] = sc.makeRDD(Array("hello world", "hello spark"))

    // 所有RDD里的算子都是由 Executor 进行执行
    // map 算子
    System.out.println("=【map】操作 及 map操作结果打印 =======================")
    val mapRDD: RDD[String] = listRDD.map(_ + "_1")
    mapRDD.collect().foreach(println)

    // flatMap 算子
    System.out.println("=【flatMap】操作 及 flatMap操作结果打印 =======================")
    val wordRDD: RDD[String] = listRDD.flatMap(_.split(" "))
    wordRDD.collect().foreach(println)
  }
}
