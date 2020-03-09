package com.java.spark_sql;

import com.scala.spark_sql.MyAgeAvgFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-09 12:58
 * @Version: 1.0
 * @Modified By:
 * @Description: 弱类型用户自定义聚合函数示例
 */
/*
弱类型用户自定义聚合函数：通过继承UserDefinedAggregateFunction来实现用户自定义聚合函数。
 */
public class Java_SparkSQL04_UDAF {
    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("SparkSQL04_UDAF");
        // JavaSparkContext sc = new JavaSparkContext(config);

        //SparkSession session = new SparkSession(sc.sc()); // 方法私有，不能正常创建
        SparkSession session = SparkSession.builder().config(config).getOrCreate();

        //进行转换前，需要引入隐式转换规则。
        //这里spark_session不是包的名字，是SparkSession的对象
        //import session.implicits._   //无论自己是否需要隐式转换，最好还是加上

        //用户自定义聚合函数
        //1. 创建聚合函数对象
        //val udf = new MyAgeAvgFunction
        MyAgeAvgFunction udf = new MyAgeAvgFunction();
        //2. 注册聚合函数
        //session.udf.register("avgAge",udf);
        session.udf().register("avgAge",udf);
        //3. 使用聚合函数
        //var frame: DataFrame = session.read.json("in/user.json")
        //frame.createOrReplaceTempView("user")
        Dataset<Row> frame = session.read().json("in/user.json");
        frame.createOrReplaceTempView("user");

        //session.sql("select avgAge(age) from user").show
        session.sql("select avgAge(age) from user").show();

        // 释放资源
        session.stop();
    }
}
