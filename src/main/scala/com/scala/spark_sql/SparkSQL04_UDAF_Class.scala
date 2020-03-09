package com.scala.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-09 14:06
 * @Version: 1.0
 * @Modified By:
 * @Description: 自定义强类型用户自定义聚合函数
 */

/*
强类型用户自定义聚合函数：通过继承Aggregator来实现强类型自定义聚合函数
 */

object SparkSQL04_UDAF_Class {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL04_UDAF_Class")

    // 创建spark上下文对象
    // var session: SparkSession = new SparkSession(config) // 方法私有，不能正常创建
    val session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    //进行转换前，需要引入隐式转换规则。
    //这里spark_session不是包的名字，是SparkSession的对象
    import session.implicits._ //无论自己是否需要隐式转换，最好还是加上

    //用户自定义聚合函数
    //1. 创建聚合函数对象
    val udf = new MyAgeAvgClassFunction

    //2. 将聚合函数转换为查询列，因为传入的是对象
    var avgCol: TypedColumn[UserBean, Double] = udf.toColumn.name("avgAge")
    //读取文件
    var frame: DataFrame = session.read.json("in/user.json")
    //转换位dataset,DSL风格
    var userDS: Dataset[UserBean] = frame.as[UserBean]

    //应用函数，因为传入的是对象，并且每条进行处理
    userDS.select(avgCol).show()

    // 释放资源
    session.stop()
  }

}



case class UserBean(name: String, age: BigInt)
//这里输入 数据类型 需要改为BigInt，不能为Int。因为程序读取文件的时候，不能判断int类型到底多大，所以会报错 truncate

case class AvgBuffer(var sum: BigInt, var count: Int)

// 声明用户自定义聚合函数(强类型)
// 1, 继承：Aggregator,设定泛型
// 2,实现方法
class MyAgeAvgClassFunction extends Aggregator[UserBean, AvgBuffer, Double] {

  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  //聚合数据
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  //缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  //完成计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count.toDouble
  }

  //数据类型转码，自定义类型 基本都是Encoders.product
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  //基本数据类型：Encoders.scala。。。
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}