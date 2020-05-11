package com.scala.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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
    //    val startTime = LocalDateTime.now()
    //
    //    TimeUnit.SECONDS.sleep(80)
    //    val endTime = LocalDateTime.now;
    //    // 此日期时间与结束日期时间之间的时间量
    //    val i = startTime.until(endTime, ChronoUnit.SECONDS)
    //
    //    println(i)

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
    var rdd: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List((1, "zhangsan", 10), (2, "lisi", 20), (3, "wangwu", 30)))


    val studentDs: Dataset[Coltest] = rdd.map {
      case (id, name, age) =>
        Coltest(id, name, age)
    }.toDS()

    studentDs.map(colTest => {
      println(colTest.name)
      println(colTest.id)
      colTest
    })

    val filterDataSet: Dataset[Coltest] = studentDs.filter(colTest => {
      colTest.id > 1
    })

    filterDataSet.foreach( colTest => {
      print(colTest.name)
      println("colTest.name:" + colTest.name + "\tcolTest.id:" + colTest.id)
    })

    // 注册成临时表进行查询
    studentDs.createOrReplaceTempView("ods_student")
    val frame1: DataFrame = studentDs.sqlContext.sql("select * from ods_student where id < 3")
    println("=====================================")
    frame1.show()

    val frame2: DataFrame = session.sql("select * from ods_student where id < 3")
    println("=====================================")
    frame2.show()

    // 关闭资源
    session.stop()
  }
}


case class Coltest(id: Int, name: String, age: Int) extends Serializable