package com.java.spark.file_read_save;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.JdbcRDD;
import scala.Tuple2;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 2:19 下午 2020/3/7
 * @Version: 1.0
 * @Modified By:
 * @Description: spark core操作mysql，支持通过Java JDBC访问关系型数据库。需要通过JdbcRDD进行
 */

/*

pom.xml添加mysql的jdbc连接驱动

<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.27</version>
</dependency>
 */
public class JAVA_Spark18_Mysql {
    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("JAVA_Spark18_Mysql");
        JavaSparkContext sc = new JavaSparkContext(config);


        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/spark_learn_rdd";
        String userName = "root";
        String passMd = "1234";

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
        sc.stop();
    }


    private static void selectFromMySQL(JavaSparkContext sc, String driver, String url, String userName, String passMd) {
        //创建jdbcRDD，方法数据库,查询数据
        String sql = "select name, age from user where id >= ? and id <= ?";

        JavaRDD<Port> jdbcRDD = JdbcRDD.create(
                sc,
                () -> {
                    //获取数据库连接对象
                    try {
                        Class.forName(driver);
                        return DriverManager.getConnection(url, userName, passMd);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                },
                sql,
                1,
                3,
                2,
                // rs -> {
                //    //println(rs.getString(1) + "  ,  " + rs.getInt(2))
                //}
                new Function<ResultSet, Port>() {
                    private static final long serialVersionUID = 1L;

                    public Port call(ResultSet rs) throws Exception {
                        ResultSetMetaData meta = rs.getMetaData();
                        Port port = new Port();
                        int columns = meta.getColumnCount();
                        for (int i = 1; i <= columns; i++) {
                            PropertyUtils.setProperty(port,
                                    meta.getColumnLabel(i).toLowerCase(),
                                    rs.getString(i));
                        }
                        return port;
                    }
                }
        );
        jdbcRDD.collect().forEach(System.out::println);
    }


    private static void saveDataToMysqlWayOne(JavaSparkContext sc, String driver, String url, String userName, String passMd) {
        //  保存数据
        //var dataRDD:RDD[(String, Int)] = sc.makeRDD(List(("zongzhong", 23), ("lishi", 23)))
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>("zongzhong", 23));
        list.add(new Tuple2<>("lishi", 23));
        JavaPairRDD<String, Integer> dataRDD = sc.parallelizePairs(list);

        //Class.forName(driver)
        //val conection = java.sql.DriverManager.getConnection(url, userName, passMd)
        /*
        dataRDD.foreach {
            case (name, age) =>{
                Class.forName(driver)
                val conection = java.sql.DriverManager.getConnection(url, userName, passMd)
                val sql = "insert into  user (name, age) values (?,?)"
                var statement:PreparedStatement = conection.prepareStatement(sql)
                var usern:Unit = statement.setString(1, name)
                val pasword = statement.setInt(2, age)
                statement.executeUpdate()
                statement.close()
                conection.close()

            }
        } */
        dataRDD.foreach(tuple -> {
            Class.forName(driver);
            Connection conn = java.sql.DriverManager.getConnection(url, userName, passMd);
            String sql = "insert into  user (name, age) values (?,?)";
            PreparedStatement statement = conn.prepareStatement(sql);
            statement.setString(1, tuple._1);
            statement.setInt(2, tuple._2);
            statement.executeUpdate();
            statement.close();
            conn.close();
        });
    }


    private static void saveDataToMysqlWayTwo(JavaSparkContext sc, String driver, String url, String userName, String passMd) {
        //  保存数据
        //var dataRDD:RDD[(String, Int)] =sc.makeRDD(List(("zongzhong", 23), ("lishi", 23)))
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>("zongzhong", 23));
        list.add(new Tuple2<>("lishi", 23));
        JavaPairRDD<String, Integer> dataRDD = sc.parallelizePairs(list);

        /*
          dataRDD.foreachPartition(datas => { //以分区作为循环，发送到 excutor上，如果有两个分区，则直接发送到excuator上
          Class.forName(driver)
          val conn = java.sql.DriverManager.getConnection(url, userName, passMd)
          val sql = "insert into  user (name, age) values (?,?)"
          var statement: PreparedStatement = conn.prepareStatement(sql)
          datas.foreach {
            case (name, age) => {
              statement.setString(1, name)
              statement.setInt(2, age)
              statement.executeUpdate()
            }
          }
          statement.close()
          conn.close() */
        dataRDD.foreachPartition(datasIterator -> {
            Class.forName(driver);
            Connection conn = java.sql.DriverManager.getConnection(url, userName, passMd);
            String sql = "insert into  user (name, age) values (?,?)";
            PreparedStatement statement = conn.prepareStatement(sql);
            while (datasIterator.hasNext()) {
                Tuple2<String, Integer> next = datasIterator.next();
                statement.setString(1, next._1);
                statement.setInt(2, next._2);
                statement.executeUpdate();
            }
            statement.close();
            conn.close();
        });
    }

}
