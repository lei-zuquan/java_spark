package com.bigdata.spark.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 6:44 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: combineByKey算子 作用：对相同K，把V合并成一个集合。
 */
object Spark13_Oper12_6_combineByKey {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("combineByKey")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    /**
     * 需求：创建一个pairRDD，根据key计算每种key的均值。（先计算每个key出现的次数以及可以对应值的总和，再相除得到结果）
     */
    // 创建一个pairRDD
    val input = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    // 将相同key对应的值相加，同时记录该key出现的次数，放入一个二元组
    /**
     * (createCombiner: V => C,  mergeValue: (C, V) => C,  mergeCombiners: (C, C) => C)
     * 参数描述：
     * （1）createCombiner: combineByKey() 会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，要么就和之前的某个元素的键相同。如果这是一个新的元素,combineByKey()会使用一个叫作createCombiner()的函数来创建那个键对应的累加器的初始值
     * （2）mergeValue: 如果这是一个在处理当前分区之前已经遇到的键，它会使用mergeValue()方法将该键的累加器对应的当前值与这个新的值进行合并
     * （3）mergeCombiners: 由于每个分区都是独立处理的， 因此对于同一个键可以有多个累加器。如果有两个或者更多的分区都有对应同一个键的累加器， 就需要使用用户提供的 mergeCombiners() 方法将各个分区的结果进行合并。
     */
    val combine = input.combineByKey(
      (_, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    // 打印合并后的结果
    combine.collect.foreach(println)
    println("--------------------------")

    // 计算平均值
    val result = combine.map { case (key, value) => (key, value._1 / value._2.toDouble) }

    // 打印结果
    result.collect().foreach(println);

  }

}
