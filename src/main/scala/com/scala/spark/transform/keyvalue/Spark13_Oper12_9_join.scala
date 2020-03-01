package com.scala.spark.transform.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 8:21 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: join算子(性能较低) 作用：在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD
 */
object Spark13_Oper12_9_join {

  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("join")

    // 创建Spark上下文对ß象
    var sc: SparkContext = new SparkContext(config)

    /**
     * 需求：创建两个pairRDD，并将key相同的数据聚合到一个元组。
     */
    // 创建第一个pairRDD
    val rdd = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))

    // 创建第二个pairRDD
    val rdd1 = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))

    // join操作并打印结果
    rdd.join(rdd1).collect().foreach(println)

  }
}
