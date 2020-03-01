package com.scala.spark.transform.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * reduceByKey和groupByKey的区别
 *      1. reduceByKey：按照key进行聚合，在shuffle之前有combine（本地预聚合）操作，返回结果是RDD[k,v].
 *      2. groupByKey：按照key进行分组，直接进行shuffle。
 *      3. 开发指导：reduceByKey比groupByKey，建议使用。但是需要注意是否会影响业务逻辑。
 */
object Spark13_Oper12_3_reduceByKey {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("reduceByKey");

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    val rdd = sc.parallelize(List(("female", 1), ("male", 5), ("female", 5), ("male", 2)))
    val reduce = rdd.reduceByKey((x, y) => x + y)

    reduce.collect().foreach(println)

  }
}
