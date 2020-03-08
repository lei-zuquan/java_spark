package com.java.spark_sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 5:14 下午 2020/3/8
 * @Version: 1.0
 * @Modified By:
 * @Description: sparkSQL RDD、DataFrame、DataSet相互之间转换
 */
public class Java_SparkSQL03_transform {
    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("SparkSQL03_transform");
        JavaSparkContext sc = new JavaSparkContext(config);


        //SparkSession session = new SparkSession(sc.sc()); // 方法私有，不能正常创建
        SparkSession session = SparkSession.builder().config(config).getOrCreate();

        // 创建RDD
        // var rdd: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List((1, "zhagnshan", 10), (2, "lisi", 20)))
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        list.add(new Tuple2<>(1, "a"));
        list.add(new Tuple2<>(2, "b"));
        list.add(new Tuple2<>(3, "c"));
        JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(list, 2);

        // 进行转换前，需要引入隐式转换规则。
        // 这里spark_session不是包的名字，是SparkSession的对象
        //import session.implicits._;

        // 转换为DF
        //var df:DataFrame = rdd.toDF("id", "name", "age")
        // Encoders are created for Java beans
        People people = new People();
        people.setName("Andy");
        people.setId(1);

        Encoder<People> personEncoder = Encoders.bean(People.class);
        // Dataset<Row> dataFrame = session.createDataFrame(list, People.class);
        Dataset<Row> df = session.createDataFrame(Collections.singletonList(people), People.class);

        df.show();

        // 转换为DS
        // var ds: Dataset[User] = df.as[User] //这里需要创建样例类
        Encoder<People> peopleEncoder = Encoders.bean(People.class);
        Dataset<People> ds = df.as(peopleEncoder);
        // 转换为DF
        // var df1:DataFrame = ds.toDF()
        Dataset<Row> df1 = ds.toDF();
        // 转换为RDD
        //var rdd1:RDD[Row] = df1.rdd //这里是 row 类型需要注意
        RDD<Row> rdd1 = df1.rdd();
        Row[] rows = rdd1.collect();
        for (Row row : rows) {
            System.out.print(row.getInt(0) + "\t");
            System.out.print(row.getString(1) + "\t");
        }

//        rdd1.foreach(row -> {
//            // 获取数据时，可以通过索引访问数据
//            System.out.print(row.getInt(0) + "\t");
//            System.out.print(row.getString(1) + "\t");
//            // System.out.println(row.getInt(2) + "\t");
//            return null;
//        });

        // 释放资源
        session.stop();
    }
}

