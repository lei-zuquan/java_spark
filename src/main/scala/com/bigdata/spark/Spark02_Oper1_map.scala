package com.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark02_Oper1_map {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    // map算子
    var listRDD: RDD[Int] = sc.makeRDD(1 to 10) //这里的to 是包含  10的， unto 是不包含10 的

    // 所有RDD里的算子都是由Execuator进行执行
    val mapRDD:RDD[Int] = listRDD.map(_*2)
    mapRDD.collect().foreach(println)

  }
}
