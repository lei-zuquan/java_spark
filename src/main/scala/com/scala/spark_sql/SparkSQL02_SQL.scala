package com.scala.spark_sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 4:47 下午 2020/3/8
 * @Version: 1.0
 * @Modified By:
 * @Description: sparkSQL操作dataFrame示例
 */
object SparkSQL02_SQL {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL02_SQL")

    // 创建spark上下文对象
    var sc: SparkContext = new SparkContext(config)
    // var session: SparkSession = new SparkSession(config) // 方法私有，不能正常创建
    val session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    //读取数据，构建DataFrame
    var frame: DataFrame = session.read.json("in/user.json")

    //将DataFrame转成一张表
    frame.createOrReplaceTempView("user")
    session.sql("select * from user").show

    //展示数据
    frame.show()

    // 释放资源
    session.stop()
  }

}
