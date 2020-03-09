package com.scala.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-09 12:52
 * @Version: 1.0
 * @Modified By:
 * @Description: 弱类型用户自定义聚合函数示例
 */

/*
弱类型用户自定义聚合函数：通过继承UserDefinedAggregateFunction来实现用户自定义聚合函数。
 */

object SparkSQL04_UDAF {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL04_UDAF")

    // 创建spark上下文对象
    // var session: SparkSession = new SparkSession(config) // 方法私有，不能正常创建
    val session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    //创建RDD
    var rdd: RDD[(Int, String)] = session.sparkContext.makeRDD(List((1, "zhagnshan"), (2, "lisi")))
    //进行转换前，需要引入隐式转换规则。
    //这里spark_session不是包的名字，是SparkSession的对象
    import session.implicits._   //无论自己是否需要隐式转换，最好还是加上

    //用户自定义聚合函数
    //1. 创建聚合函数对象
    val udf = new MyAgeAvgFunction
    //2. 注册聚合函数
    session.udf.register("avgAge",udf);
    //3. 使用聚合函数
    var frame: DataFrame = session.read.json("in/user.json")
    frame.createOrReplaceTempView("user")

    session.sql("select avgAge(age) from user").show

    // 释放资源
    session.stop()
  }

}


// 声明用户自定义聚合函数（弱类型）
// 1, 继承：UserDefinedAggregateFunction
// 2,实现方法
class MyAgeAvgFunction extends UserDefinedAggregateFunction{

  // 函数输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)
  }
  // 计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }
  // 函数返回的数据类型
  override def dataType: DataType = DoubleType

  // 函数是否稳定: 给相同的值，在不同的时间，结果是否一致
  override def deterministic: Boolean = true

  // 计算前缓存区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 没有名称，只有结构，只能通过标记位来确定
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 根据查询结果  更新数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) +1
  }

  // 将多个节点的缓冲区进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //sum
    buffer1(0) = buffer1.getLong(0)+buffer2.getLong(0)
    //count
    buffer1(1) =  buffer1.getLong(1)+buffer2.getLong(1)
  }

  // 计算逻辑
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1).toDouble
  }
}

