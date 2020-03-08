package com.scala.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 3:29 下午 2020/3/8
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * pom.xml文件添加依赖
 *
 * <dependency>
 * <groupId>org.apache.spark</groupId>
 * <artifactId>spark-sql_2.11</artifactId>
 * <version>2.1.1</version>
 * </dependency>
 *
 */
object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    // 创建spark上下文对象
    // var session: SparkSession = new SparkSession(config) // 方法私有，不能正常创建
    val session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    //读取数据，构建DataFrame
    var frame: DataFrame = session.read.json("in/user.json")

    //展示数据
    frame.show()

    // 释放资源
    session.stop()
  }

}
