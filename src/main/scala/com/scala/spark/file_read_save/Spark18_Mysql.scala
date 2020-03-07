package com.scala.spark.file_read_save

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:56 上午 2020/3/7
 * @Version: 1.0
 * @Modified By:
 * @Description: spark core操作mysql，支持通过Java JDBC访问关系型数据库。需要通过JdbcRDD进行
 */

/*
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.27</version>
</dependency>
 */
object Spark18_Mysql {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark18_Mysql")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/spark_learn_rdd"
    val userName = "root"
    val passMd = "1234"

    /*
        spark 操作mysql数据

        DROP DATABASE IF EXISTS `spark_learn_rdd`;
        CREATE DATABASE IF NOT EXISTS `spark_learn_rdd` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

        USE `spark_learn_rdd`;
        DROP TABLE IF EXISTS `user`;

        CREATE TABLE `user` (
          `id` INT(11) NOT NULL AUTO_INCREMENT,
          `name` VARCHAR(255) DEFAULT NULL,
          `age` INT(11),
          PRIMARY KEY (`id`)
        ) ENGINE=INNODB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;

        INSERT INTO `user` (`id`, `name`, `age`) VALUES
        (1,'zhangsan', 20),
        (2,'lisi', 30),
        (3,'wangwu', 40)
     */

    //创建jdbcRDD，方法数据库,查询数据
    selectFromMySQL(sc, driver, url, userName, passMd);

    // 保存数据方式一
    saveDataToMysqlWayOne(sc, driver, url, userName, passMd);

    // 保存数据方式二
    saveDataToMysqlWayTwo(sc, driver, url, userName, passMd);

    //释放资源
    sc.stop()
  }

  def selectFromMySQL(sc: SparkContext, driver: String, url: String, userName: String, passMd: String): Unit = {
    //创建jdbcRDD，方法数据库,查询数据
    val sql = "select name, age from user where id >= ? and id <= ?"
    var jdbcRDD = new JdbcRDD(
      sc,
      () => {
        //获取数据库连接对象
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passMd)
      },
      sql,
      1,
      3,
      2,
      (rs) => {
        println(rs.getString(1) + "  ,  " + rs.getInt(2))
      }
    )
    jdbcRDD.collect()
  }

  def saveDataToMysqlWayOne(sc: SparkContext, driver: String, url: String, userName: String, passMd: String): Unit = {
    ;
    //  保存数据
    var dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zongzhong", 23), ("lishi", 23)))

    //Class.forName(driver)
    //val conection = java.sql.DriverManager.getConnection(url, userName, passMd)
    dataRDD.foreach {
      case (name, age) => {
        Class.forName(driver)
        val conection = java.sql.DriverManager.getConnection(url, userName, passMd)
        val sql = "insert into  user (name, age) values (?,?)"
        var statement: PreparedStatement = conection.prepareStatement(sql)
        var usern: Unit = statement.setString(1, name)
        val pasword = statement.setInt(2, age)
        statement.executeUpdate()
        statement.close()
        conection.close()

      }
    }
  }

  def saveDataToMysqlWayTwo(sc: SparkContext, driver: String, url: String, userName: String, passMd: String): Unit = {
    //  保存数据
    var dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zongzhong", 23), ("lishi", 23)))

    dataRDD.foreachPartition(datas => { //以分区作为循环，发送到 excutor上，如果有两个分区，则直接发送到excuator上
      Class.forName(driver)
      val conection = java.sql.DriverManager.getConnection(url, userName, passMd)
      datas.foreach {
        case (name, age) => {
          // Class.forName(driver)
          // val conection = java.sql.DriverManager.getConnection(url, userName, passMd)
          val sql = "insert into  user (name, age) values (?,?)"
          var statement: PreparedStatement = conection.prepareStatement(sql)
          var usern: Unit = statement.setString(1, name)
          val pasword = statement.setInt(2, age)
          statement.executeUpdate()
          statement.close()
          conection.close()
        }
      }
    })
  }
}
