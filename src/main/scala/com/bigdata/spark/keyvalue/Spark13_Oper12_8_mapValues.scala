package com.bigdata.spark.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:58 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: mapValues算子 作用：针对于(K,V)形式的类型只对V进行操作
 */
object Spark13_Oper12_8_mapValues {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mapValues")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    /**
     * 需求：创建一个pairRDD，并将value添加字符串"|||"
     */

    // 创建一个pairRDD
    val rdd3 = sc.parallelize(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))

    // 对value添加字符串"|||"
    rdd3.mapValues(_ + "|||").collect().foreach(println)

  }

}
