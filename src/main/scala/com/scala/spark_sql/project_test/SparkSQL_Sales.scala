package com.scala.spark_sql.project_test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:50 下午 2020/3/10
 * @Version: 1.0
 * @Modified By:
 * @Description: 使用sparkSQL统计销售定单示例
 */


object SparkSQL_Sales {
  def main(args: Array[String]): Unit = {

    // 创建配置对象
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_Sales")

    // 创建sparkSession对象
    // var session: SparkSession = new SparkSession(config) // 方法私有，不能正常创建
    val session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    //val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      //.config("spark.sql.warehouse.dir", warehouseLocation) // 使用内置Hive需要指定一个Hive仓库地址。若使用的是外部Hive，则需要将hive-site.xml添加到ClassPath下。
      .enableHiveSupport()
      .getOrCreate()

    // 进行转换前，需要引入隐式转换规则。
    // 这里spark_session不是包的名字，是SparkSession的对象
    import spark.implicits._ //无论自己是否需要隐式转换，最好还是加上
    val tbStockRdd = spark.sparkContext.textFile("in/spark_sql_input_file/tbStock.txt")

    val value1: RDD[tbStock] = tbStockRdd.map(line => {
      val splits = line.split(",")
      tbStock(splits(0), splits(1), splits(2))
    })

    val tbStockDS = tbStockRdd.map(_.split(",")).map(attr => tbStock(attr(0), attr(1), attr(2))).toDS
    tbStockDS.show()


    val tbStockDetailRdd = spark.sparkContext.textFile("in/spark_sql_input_file/tbStockDetail.txt")
    val tbStockDetailDS = tbStockDetailRdd.map(_.split(",")).map(attr => tbStockDetail(attr(0), attr(1).trim().toInt, attr(2), attr(3).trim().toInt, attr(4).trim().toDouble, attr(5).trim().toDouble)).toDS
    tbStockDetailDS.show()


    val tbDateRdd = spark.sparkContext.textFile("in/spark_sql_input_file/tbDate.txt")
    val tbDateDS = tbDateRdd.map(_.split(",")).map(attr => tbDate(attr(0), attr(1).trim().toInt, attr(2).trim().toInt, attr(3).trim().toInt, attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt, attr(7).trim().toInt, attr(8).trim().toInt, attr(9).trim().toInt)).toDS
    tbDateDS.show()

    // 注册表：
    tbStockDS.createOrReplaceTempView("tbStock")
    tbDateDS.createOrReplaceTempView("tbDate")
    tbStockDetailDS.createOrReplaceTempView("tbStockDetail")

    /*
    4.3 计算所有订单中每年的销售单数、销售总额
        统计所有订单中每年的销售单数、销售总额
        三个表连接后以count(distinct a.ordernumber)计销售单数，sum(b.amount)计销售总额
        SELECT c.theyear, COUNT(DISTINCT a.ordernumber), SUM(b.amount)
        FROM tbStock a
          JOIN tbStockDetail b ON a.ordernumber = b.ordernumber
          JOIN tbDate c ON a.dateid = c.dateid
        GROUP BY c.theyear
        ORDER BY c.theyear
     */
    spark.sql("SELECT c.theyear, COUNT(DISTINCT a.ordernumber), SUM(b.amount) FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear ORDER BY c.theyear").show


    /*
    4.4 计算所有订单每年最大金额订单的销售额
        目标：统计每年最大金额订单的销售额:
     */
    // 1）统计每年，每个订单一共有多少销售额
    spark.sql("SELECT a.dateid, a.ordernumber, SUM(b.amount) AS SumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber GROUP BY a.dateid, a.ordernumber").show

    /* 2）以上一步查询结果为基础表，和表tbDate使用dateid join，求出每年最大金额订单的销售额
        SELECT theyear, MAX(c.SumOfAmount) AS SumOfAmount
        FROM (SELECT a.dateid, a.ordernumber, SUM(b.amount) AS SumOfAmount
        FROM tbStock a
        JOIN tbStockDetail b ON a.ordernumber = b.ordernumber
        GROUP BY a.dateid, a.ordernumber
        ) c
        JOIN tbDate d ON c.dateid = d.dateid
        GROUP BY theyear
        ORDER BY theyear DESC
    */
    spark.sql("SELECT theyear, MAX(c.SumOfAmount) AS SumOfAmount FROM (SELECT a.dateid, a.ordernumber, SUM(b.amount) AS SumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber GROUP BY a.dateid, a.ordernumber ) c JOIN tbDate d ON c.dateid = d.dateid GROUP BY theyear ORDER BY theyear DESC").show

    // 4.5 计算所有订单中每年最畅销货品
    //目标：统计每年最畅销货品（哪个货品销售额amount在当年最高，哪个就是最畅销货品）
    /*
    第一步、求出每年每个货品的销售额
    SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount
    FROM tbStock a
    JOIN tbStockDetail b ON a.ordernumber = b.ordernumber
    JOIN tbDate c ON a.dateid = c.dateid
    GROUP BY c.theyear, b.itemid

     */

    spark.sql("SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid").show

    // 第二步、在第一步的基础上，统计每年单个货品中的最大金额
    /*
    SELECT d.theyear, MAX(d.SumOfAmount) AS MaxOfAmount
    FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount
    FROM tbStock a
    JOIN tbStockDetail b ON a.ordernumber = b.ordernumber
    JOIN tbDate c ON a.dateid = c.dateid
    GROUP BY c.theyear, b.itemid
    ) d
    GROUP BY d.theyear

     */

    spark.sql("SELECT d.theyear, MAX(d.SumOfAmount) AS MaxOfAmount FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) d GROUP BY d.theyear").show

    // 第三步、用最大销售额和统计好的每个货品的销售额join，以及用年join，集合得到最畅销货品那一行信息
    /*
    SELECT DISTINCT e.theyear, e.itemid, f.MaxOfAmount
    FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount
    FROM tbStock a
    JOIN tbStockDetail b ON a.ordernumber = b.ordernumber
    JOIN tbDate c ON a.dateid = c.dateid
    GROUP BY c.theyear, b.itemid
    ) e
    JOIN (SELECT d.theyear, MAX(d.SumOfAmount) AS MaxOfAmount
    FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount
    FROM tbStock a
    JOIN tbStockDetail b ON a.ordernumber = b.ordernumber
    JOIN tbDate c ON a.dateid = c.dateid
    GROUP BY c.theyear, b.itemid
    ) d
    GROUP BY d.theyear
    ) f ON e.theyear = f.theyear
    AND e.SumOfAmount = f.MaxOfAmount
    ORDER BY e.theyear

     */

    spark.sql("SELECT DISTINCT e.theyear, e.itemid, f.maxofamount FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS sumofamount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) e JOIN (SELECT d.theyear, MAX(d.sumofamount) AS maxofamount FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS sumofamount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) d GROUP BY d.theyear ) f ON e.theyear = f.theyear AND e.sumofamount = f.maxofamount ORDER BY e.theyear").show

    // 释放资源
    spark.stop()

  }

}

case class tbStock(ordernumber: String, locationid: String, dateid: String) extends Serializable

case class tbStockDetail(ordernumber: String, rownum: Int, itemid: String, number: Int, price: Double, amount: Double) extends Serializable

case class tbDate(dateid: String, years: Int, theyear: Int, month: Int, day: Int, weekday: Int, week: Int, quarter: Int, period: Int, halfmonth: Int) extends Serializable
