package com.scala.spark_core.accumulator_broadcast

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:07 上午 2020/3/8
 * @Version: 1.0
 * @Modified By:
 * @Description: 自定义累加器
 */

/*
自定义累加器类型的功能在1.X版本中就已经提供了，但是使用起来比较麻烦，在2.0版本后，累加器的易用性有了较大的改进，
而且官方还提供了一个新的抽象类：AccumulatorV2来提供更加友好的自定义类型累加器的实现方式。实现自定义类型累加器
需要继承AccumulatorV2并至少覆写下例中出现的方法，下面这个累加器可以用于在程序运行过程中收集一些文本类信息，最
终以Set[String]的形式返回。
 */
object Spark21_Accumulator {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark21_Accumulator")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    val dataRDD: RDD[String] = sc.makeRDD(List("Hadoop", "hive", "Habsde", "scala", "hello"), 2)

    //    var sum :Int  = 0
    //    //使用累加器来共享变量，来累加数据
    //
    //    //创建累加器对象
    //    var accumulator: LongAccumulator = sc.longAccumulator

    // TODO 创建累加器
    val wordAccumulator = new WordAccumulator
    // TODO 注册累加器
    sc.register(wordAccumulator)

    dataRDD.foreach {
      case word => {
        // TODO 执行累加器的累加功能
        wordAccumulator.add(word)
      }
    }
    // TODO 获取累加器的值
    println("sum =  " + wordAccumulator.value)
    //释放资源
    sc.stop()
  }
}

// 声明累加器
// 1. 继承一个累加器抽象类AccumulatorV2 ，
// 2. 实现抽象方法
// 声明(创建）累加器
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

  val list = new util.ArrayList[String]()

  // 当前累加器是否是初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  // 复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  // 重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  // 向累加器中增加数据
  override def add(v: String): Unit = {
    if (v.contains("h")) {
      list.add(v)
    }
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  // 获取累加器的结果
  override def value: util.ArrayList[String] = {
    list
  }
}
