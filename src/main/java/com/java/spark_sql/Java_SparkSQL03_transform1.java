package com.java.spark_sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-09 11:51
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class Java_SparkSQL03_transform1 {
    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("Java_SparkSQL03_transform1");
        JavaSparkContext sc = new JavaSparkContext(config);
        
        //SparkSession session = new SparkSession(sc.sc()); // 方法私有，不能正常创建
        SparkSession session = SparkSession.builder().config(config).getOrCreate();

        //创建RDD
        //var rdd: RDD[(Int, String)] = spark_session.sparkContext.makeRDD(List((1, "zhagnshan"), (2, "lisi")))
        List<User> list = new ArrayList<>();
        list.add(new User(1, "zhangsan", 10));
        list.add(new User(2, "lisi", 20));
        list.add(new User(3, "wangwu", 30));
        JavaRDD<User> userRDD = sc.parallelize(list, 2);
        //进行转换前，需要引入隐式转换规则。
        //这里spark_session不是包的名字，是SparkSession的对象
        //import spark_session.implicits._   //无论自己是否需要隐式转换，最好还是加上


        //RDD --直接转-  DataSet，要求RDD，同时要有结构，同时要有类型
        /*
        var userRDD: RDD[User2] = rdd.map {
            case (id, name) => {
                User2(id, name)
            }
        }

        var userDS: Dataset[User2] = userRDD.toDS()
        val rdd1:RDD[User2] = userDS.rdd

        rdd1.foreach(println) */
        Encoder<User> userEncoder = Encoders.bean(User.class);
        Dataset<User> userDS = session.createDataset(list, userEncoder);
        JavaRDD<User> rdd1 = userDS.javaRDD();
        userDS.show();
        rdd1.foreach( user -> {
            if (user != null){
                System.out.println(user);
            }

        });

        // 释放资源
        //sc.close();
        session.stop();
    }
}
