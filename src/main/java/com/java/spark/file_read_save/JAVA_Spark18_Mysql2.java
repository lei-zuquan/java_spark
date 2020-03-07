package com.java.spark.file_read_save;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 2:42 下午 2020/3/7
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */


public class JAVA_Spark18_Mysql2 {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf();
        conf.setAppName("JAVA_Spark18_Mysql2").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        class MyConnectionFactory implements JdbcRDD.ConnectionFactory {
            private static final long serialVersionUID = 1L;

            @Override
            public Connection getConnection() {
                try {
                    Class.forName("com.mysql.jdbc.Driver"); //("oracle.jdbc.driver.OracleDriver");
                    String url = "jdbc:mysql://localhost:3306/spark_learn_rdd"; //"jdbc:oracle:thin:@172.168.27.6:1521:orclnew";
                    return DriverManager.getConnection(url, "root", "1234");
                } catch (Exception e){
                    e.printStackTrace();
                }
                return null;
            }

        }

        String sql = "select id, name, age from user where id >= ? and id <= ?";
        JavaRDD<UserBean> portRdd = JdbcRDD.create(jsc,
                new MyConnectionFactory(),
                sql,
                1,
                10,
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
                }).persist(StorageLevel.MEMORY_ONLY());

        System.out.println("number is:" + portRdd.count());

        JavaPairRDD<String, String> portPairRdd = portRdd.mapToPair(new PairFunction<UserBean, String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(UserBean t) throws Exception {
                return new Tuple2<String, String>(t.getName(), t.toString());
            }
        });

        // Long numAs = logData.filter(new Function<String,Boolean>(){
        // public Boolean call(String s){
        // return s.contains("a");
        // }
        // }).count();
        for (Tuple2<String, String> user : portPairRdd.cache().collect()) {
            System.out.println("key:" + user._1 + ",value:" + user._2);
        }

        FileSystem sys = FileSystem.getLocal(new Configuration());
        String pathChar = "in/char";
        String pathByte = "in/byte";
        Path p = new Path(pathChar);
        if (sys.exists(p)) {
            sys.delete(p, true);
        }
        portRdd.saveAsTextFile(pathChar);

        p = new Path(pathByte);
        if (sys.exists(p)) {
            sys.delete(p, true);
        }
        portRdd.saveAsObjectFile(pathByte);

    }
}

