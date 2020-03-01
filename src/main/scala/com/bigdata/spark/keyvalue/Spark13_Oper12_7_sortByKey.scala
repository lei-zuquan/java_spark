package com.bigdata.spark.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:36 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: sortByKey算子 作用：在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的(K,V)的RDD
 */
object Spark13_Oper12_7_sortByKey {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sortByKey")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    /**
     * 需求：创建一个pairRDD，按照key的正序和倒序进行排序
     */
    val rdd = sc.parallelize(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))

    // 按照key的正序
    rdd.sortByKey(true).collect().foreach(println)

    println("--------上面是按key的正序，下面是按key的倒序-------------------")
    // 按照key的倒序
    rdd.sortByKey(false).collect().foreach(println)
  }

}
