package com.bigdata.spark.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 8:44 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: cogroup算子(类似leftOutJoin) 作用：在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
 */
object Spark13_Oper12_10_cogroup {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cogroup")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    /**
     * 需求：创建两个pairRDD，并将key相同的数据聚合到一个迭代器。
     */
    // 创建第一个pairRDD
    val rdd = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c"), (4, "d")))

    // 创建第二个pairRDD
    val rdd1 = sc.parallelize(Array((1, 4), (2, 5), (3, 6), (5, 5)))

    // cogroup两个RDD并打印结果
    rdd.cogroup(rdd1).collect().foreach(println)

  }

}
