package com.scala.spark.transform.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 5:35 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: foldByKey算子 作用：aggregateByKey的简化操作，seqop和combop相同
 */
object Spark13_Oper12_5_foldByKey {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("foldByKey")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    val rdd = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)
    /**
     * 创建一个pairRDD，计算相同key对应值的相加结果
     */
    val agg = rdd.foldByKey(0)(_ + _)

    agg.collect().foreach(println)

  }
}
