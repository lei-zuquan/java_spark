package com.scala.spark.transform.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 3:34 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: groupByKey算子 作用：groupByKey也是对每个key进行操作，但只生成一个sequence。
 */
object Spark13_Oper12_2_groupByKey {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    val words = Array("one", "two", "two", "three", "three", "three")

    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))

    val group = wordPairsRDD.groupByKey()

    group.collect().foreach(println)

    group.map(t => (t._1, t._2.sum))

    group.map(t => (t._1, t._2.sum))
  }
}
