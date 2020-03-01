package com.scala.spark.transform.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 参数：(zeroValue:U,[partitioner: Partitioner]) (seqOp: (U, V) => U,combOp: (U, U) => U)
 * 1. 作用：在kv对的RDD中，，按key将value进行分组合并，合并时，将每个value和初始值作为seq函数的参数，进行计算，
 * 返回的结果作为一个新的kv对，然后再将结果按照key进行合并，最后将每个分组的value传递给combine函数进行计算
 * （先将前两个value进行计算，将返回结果和下一个value传给combine函数，以此类推），将key与计算结果作为一个新的kv对输出。
 */
object Spark13_Oper12_4_aggregateByKey {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("aggregateByKey")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    // glom算子，将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
    rdd.glom().foreach(array => {
      println(array.mkString(","))
    })

    println("=========================")
    /**
     * 创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加
     * （1）zeroValue：给每一个分区中的每一个key一个初始值；
     * （2）seqOp：函数用于在每一个分区中用初始值逐步迭代value；
     * （3）combOp：函数用于合并每个分区中的结果。
     */
    val agg = rdd.aggregateByKey(0)(math.max(_, _), _ + _)

    agg.collect().foreach(println)

  }
}
