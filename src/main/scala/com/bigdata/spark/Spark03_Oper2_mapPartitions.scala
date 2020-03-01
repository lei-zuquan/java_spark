package com.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:47 上午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
object Spark03_Oper2_mapPartitions {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mapPartitions")

    //创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    //map算子
    var listRDD: RDD[Int] = sc.makeRDD(1 to 10) //这里的to 是包含  10的， unto 是不包含10 的

    //mapPartitions 可以对一个RDD中的所有分区进行遍历
    //mapPartitions 效率优于map算子，减少发送到执行器执行的交换次数
    //mapPartitions 缺点是可能会出现 内存溢出（OOM)
    var mapPartitions: RDD[Int] = listRDD.mapPartitions(datas => {
      datas.map(datas => datas * 2) //这里是 scala计算，不是RDD计算，会把这个整个发送给执行器exceuter
    })

    mapPartitions.collect().foreach(println)

  }
}
