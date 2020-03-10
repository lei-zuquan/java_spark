package com.java.spark_sql;

import com.java.spark_sql.dataset06.ColTest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-10 14:54
 * @Version: 1.0
 * @Modified By:
 * @Description: DataSet示例
 */
public class Java_SparkSQL06_DataSet {
    public static void main(String[] args){
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("Java_SparkSQL06_DataSet");
        JavaSparkContext sc = new JavaSparkContext(config);

        //SparkSession session = new SparkSession(sc.sc()); // 方法私有，不能正常创建
        SparkSession session = SparkSession.builder().config(config).getOrCreate();


        List<ColTest> list = new ArrayList<>();
        list.add(new ColTest("zhangsan", 1));
        list.add(new ColTest("lisi", 2));
        list.add(new ColTest("wangwu", 3));
        JavaRDD<ColTest> rdd = sc.parallelize(list);

        Dataset<Row> df = session.createDataFrame(list, ColTest.class);
        Encoder<ColTest> colTestEncoder = Encoders.bean(ColTest.class);
        Dataset<ColTest> ds = df.as(colTestEncoder);

//        ds.map( colTest -> {
//            return colTest;
//        });

        ds.foreach( colTest -> {
            System.out.println("Id:" + colTest.getId() + "\tName:" + colTest.getName());
        });



        session.stop();

    }
}
