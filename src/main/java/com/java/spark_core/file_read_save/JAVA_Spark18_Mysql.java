package com.java.spark_core.file_read_save;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.JdbcRDD;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
        selectFromMysql(sc);
        selectFromMysqlLambda(sc);

        // 保存数据方式一
        saveDataToMysqlWayOne(sc);

        // 保存数据方式二
        saveDataToMysqlWayTwo(sc);

        //释放资源
        sc.stop();
    }

    private static void selectFromMysql(JavaSparkContext sc) {
        //创建jdbcRDD，方法数据库,查询数据
        String sql = "select id, name, age from user where id >= ? and id <= ?";

        JavaRDD<UserBean> jdbcRDD = JdbcRDD.create(
                sc,
                () -> {
                    return new MyConnectionFactory().getConnection();
                },
                sql,
                1,
                7,
                2,
                new Function<ResultSet, UserBean>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public UserBean call(ResultSet rs) throws Exception {
                        ResultSetMetaData meta = rs.getMetaData();
                        UserBean user = new UserBean();
                        int columns = meta.getColumnCount();
                        for (int i = 1; i <= columns; i++) {
                            PropertyUtils.setProperty(user,
                                    meta.getColumnLabel(i).toLowerCase(),
                                    rs.getObject(i));
                        }
                        return user;
                    }
                }
        );
        jdbcRDD.collect().forEach(System.out::println);
    }

    private static void selectFromMysqlLambda(JavaSparkContext sc) {
        //创建jdbcRDD，方法数据库,查询数据
        String sql = "select id, name, age from user where id >= ? and id <= ?";

        JavaRDD<UserBean> jdbcRDD = JdbcRDD.create(
                sc,
                () -> {
                    return new MyConnectionFactory().getConnection();
                },
                sql,
                1,
                7,
                2,
                rs -> { // ResultSet
                    ResultSetMetaData meta = rs.getMetaData();
                    UserBean user = new UserBean();
                    int columns = meta.getColumnCount();
                    for (int i = 1; i <= columns; i++) {
                        PropertyUtils.setProperty(user, meta.getColumnLabel(i).toLowerCase(), rs.getObject(i));
                    }
                    return user;
                }
        );
        jdbcRDD.collect().forEach(System.out::println);
    }


    private static void saveDataToMysqlWayOne(JavaSparkContext sc) {
        //  保存数据
        //var dataRDD:RDD[(String, Int)] = sc.makeRDD(List(("java12", 111), ("java12", 112)))
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>("java11", 111));
        list.add(new Tuple2<>("java12", 112));
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
            Connection conn = new MyConnectionFactory().getConnection();
            String sql = "insert into  user (name, age) values (?,?)";
            PreparedStatement statement = conn.prepareStatement(sql);
            statement.setString(1, tuple._1);
            statement.setInt(2, tuple._2);
            statement.executeUpdate();
            statement.close();
            conn.close();
        });
    }


    private static void saveDataToMysqlWayTwo(JavaSparkContext sc) {
        //  保存数据
        //var dataRDD:RDD[(String, Int)] =sc.makeRDD(List(("zongzhong", 23), ("lishi", 23)))
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>("java21", 221));
        list.add(new Tuple2<>("java22", 222));
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
            Connection conn = new MyConnectionFactory().getConnection();
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
