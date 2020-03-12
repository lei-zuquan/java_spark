package com.scala.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

/**
  * @Author: Lei
  * @E-mail: 843291011@qq.com
  * @Date: 2020-03-10 14:15
  * @Version: 1.0
  * @Modified By:
  * @Description: DataSet示例
  */
object SparkSQL06_DataSet {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL06_DataSet")

    // 创建spark上下文对象
    // var sc: SparkContext = new SparkContext(config)
    // var session: SparkSession = new SparkSession(config) // 方法私有，不能正常创建
    val session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    //进行转换前，需要引入隐式转换规则。
    //这里spark_session不是包的名字，是SparkSession的对象
    import session.implicits._   //无论自己是否需要隐式转换，最好还是加上

    //定义字段名和类型
    /**
     rdd
     ("a", 1)
     ("b", 1)
     ("a", 1)
      **/
    var rdd: RDD[(String, Int)]  = session.sparkContext.makeRDD(List(("zhangsan", 1), ("lisi", 2), ("wangwu", 3)))


    val ds: Dataset[Coltest] = rdd.map{
          case(col1, col2) =>
            Coltest(col1, col2)
    }.toDS()

    ds.map( colTest => {
      println(colTest.name)
      println(colTest.id)
      colTest
    })

    val filterDataSet: Dataset[Coltest] = ds.filter(colTest => {
      colTest.id > 1
    })

    filterDataSet.foreach( colTest => {
      print(colTest.name)
      println("colTest.name:" + colTest.name + "\tcolTest.id:" + colTest.id)
    })

    // 关闭资源
    session.stop()
  }
}


case class Coltest(name:String, id:Int) extends Serializable