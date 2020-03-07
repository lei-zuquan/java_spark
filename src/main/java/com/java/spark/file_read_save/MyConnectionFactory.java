package com.java.spark.file_read_save;

import org.apache.spark.rdd.JdbcRDD;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:20 下午 2020/3/7
 * @Version: 1.0
 * @Modified By:
 * @Description: 获取数据库连接对象
 */
public class MyConnectionFactory implements JdbcRDD.ConnectionFactory {
    private static final long serialVersionUID = 1L;

    private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String ORACLE_URL = "jdbc:oracle:thin:@172.168.27.6:1521:orclnew";

    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/spark_learn_rdd";
    private static final String USER_NAME = "root";
    private static final String PASS_WORD = "1234";

    @Override
    public Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName(MYSQL_DRIVER);
            conn = DriverManager.getConnection(MYSQL_URL, USER_NAME, PASS_WORD);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
}