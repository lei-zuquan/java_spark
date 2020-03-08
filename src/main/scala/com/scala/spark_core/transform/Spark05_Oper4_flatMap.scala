package com.scala.spark_core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:53 上午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: @Description: flatMap算子，作用：类似于map，但是每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素）
 */
object Spark05_Oper4_flatMap {

  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMap")

    //创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    //map算子
    var listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))

    //flatMap
    //1,2,3,4
    var flatMapRDD: RDD[Int] = listRDD.flatMap(datas => datas)

    flatMapRDD.collect().foreach(println)
  }
}
