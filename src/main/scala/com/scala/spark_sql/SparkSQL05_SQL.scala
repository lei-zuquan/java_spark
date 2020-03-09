package com.scala.spark_sql

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:10 下午 2020/3/9
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
object SparkSQL05_SQL {
  def main(args: Array[String]): Unit = {

    // 创建配置对象
    //var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL05_SQL")

    // 创建sparkSession对象
    // var session: SparkSession = new SparkSession(config) // 方法私有，不能正常创建
    // val session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    //使用Hive的操作
    //val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath //如果是内置的、需指定hive仓库地址，若使用的是外部Hive，则需要将hive-site.xml添加到ClassPath下。
    val session = SparkSession.builder()
      .appName("Spark Hive Example")
      .master("local[*]")
      // .config("spark.sql.warehouse.dir", warehouseLocation) // 如果使用spark默认hive数仓，则需要此配置
      //      .enableHiveSupport()
      .getOrCreate()
    val rdd = session.sparkContext.textFile("in/person.txt")
    //var frame: DataFrame = session.read.json("in/user.json")

    //整理数据，ROW类型
    val rowrdd = rdd.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val facevalue = fields(3).toDouble
      Row(id, name, age, facevalue)
    })

    //scheme:定义DataFrame里面元素的数据类型，以及对每个元素的约束
    val structType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("faceValue", DoubleType, true)
    ))

    // 将rowrdd和structType关联,因为文本 类 没有结构，所以
    val df: DataFrame = session.createDataFrame(rowrdd, structType)
    // 创建一个视图
    df.createOrReplaceTempView("Person")
    // 基于注册的视图写SQL
    val res: DataFrame = session.sql("SELECT id, name, age, faceValue FROM Person ORDER BY age asc")

    res.show()

    // 释放资源
    session.stop()

  }

}
