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
        conf.setAppName("JAVA_Spark18_Mysql2").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        class MyConnectionFactory implements JdbcRDD.ConnectionFactory {
            private static final long serialVersionUID = 1L;

            public Connection getConnection() throws Exception {
                Class.forName("com.mysql.jdbc.Driver"); //("oracle.jdbc.driver.OracleDriver");
                String url = "jdbc:mysql://localhost:3306/spark_learn_rdd"; //"jdbc:oracle:thin:@172.168.27.6:1521:orclnew";
                return DriverManager.getConnection(url, "root", "1234");
            }

        }

        String sql = "select name, age from user where id >= ? and id <= ?";
        JavaRDD<Port> portRdd = JdbcRDD.create(jsc,
                new MyConnectionFactory(),
                sql,
                1,
                10,
                2,
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
                }).persist(StorageLevel.MEMORY_ONLY());

        System.out.println("number is:" + portRdd.count());

        JavaPairRDD<String, String> portPairRdd = portRdd.mapToPair(new PairFunction<Port, String, String>() {
            private static final long serialVersionUID = 1L;

            public Tuple2<String, String> call(Port t) throws Exception {
                return new Tuple2<String, String>(t.getName(), t.toString());
            }
        });

        // Long numAs = logData.filter(new Function<String,Boolean>(){
        // public Boolean call(String s){
        // return s.contains("a");
        // }
        // }).count();
        for (Tuple2<String, String> port : portPairRdd.cache().collect()) {
            System.out.println("key:" + port._1 + ",value:" + port._2);
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

class Port {
    private int id;
    private String name;
    private int age;

    public Port() {
        super();
    }

    public Port(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Port{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}