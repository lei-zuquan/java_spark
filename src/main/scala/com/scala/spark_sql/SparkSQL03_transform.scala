package com.scala.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 4:58 下午 2020/3/8
 * @Version: 1.0
 * @Modified By:
 * @Description: sparkSQL RDD、DataFrame、DataSet相互之间转换
 */
object SparkSQL03_transform {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL03_transform")

    // 创建spark上下文对象
    // var sc: SparkContext = new SparkContext(config)
    // var session: SparkSession = new SparkSession(config) // 方法私有，不能正常创建
    val session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    // 创建RDD
    var rdd: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List((1, "zhagnshan", 10), (2, "lisi", 20)))
    // 进行转换前，需要引入隐式转换规则。
    // 这里spark_session不是包的名字，是SparkSession的对象
    import session.implicits._

    // 转换为DF
    var df: DataFrame = rdd.toDF("id", "name", "age")

    // 转换为DS
    var ds: Dataset[User] = df.as[User] //这里需要创建样例类
    // 转换为DF
    var df1: DataFrame = ds.toDF()
    // 转换为RDD
    var rdd1: RDD[Row] = df1.rdd //这里是 row 类型需要注意

    rdd1.foreach(row => {
      // 获取数据时，可以通过索引访问数据
      print(row.getInt(0) + "\t")
      print(row.getString(1) + "\t")
      println(row.getInt(2) + "\t")
    })

    // 释放资源
    session.stop()
  }

}


//创建样例类，DataSet需要类型
case class User(id: Int, name: String, age: Int);