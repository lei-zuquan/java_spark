package com.scala.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:55 上午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: filter算子 作用：过滤。返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成。
 */
object Spark08_Oper7_filter {

  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("filter")

    //创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    //map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。
    var listRDD: RDD[Int] = sc.makeRDD(1 to 16) //这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数

    //过滤，满足条件则取出来
    var filterRDD: RDD[Int] = listRDD.filter(x => x % 2 == 0)

    filterRDD.collect().foreach(println)
  }
}
