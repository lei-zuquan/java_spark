package com.java.spark_sql;

import com.java.spark_sql.udaf04.MyAgeAvgClassFunction;
import com.java.spark_sql.udaf04.UserBean;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-09 14:10
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class Java_SparkSQL04_UDAF_Class {
    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("SparkSQL04_UDAF_Class");
        // JavaSparkContext sc = new JavaSparkContext(config);
        
        //SparkSession session = new SparkSession(sc.sc()); // 方法私有，不能正常创建
        SparkSession session = SparkSession.builder().config(config).getOrCreate();

        //进行转换前，需要引入隐式转换规则。
        //这里spark_session不是包的名字，是SparkSession的对象
        //import session.implicits._ //无论自己是否需要隐式转换，最好还是加上

        //用户自定义聚合函数
        //1. 创建聚合函数对象
        MyAgeAvgClassFunction udf = new MyAgeAvgClassFunction();

        //2. 将聚合函数转换为查询列，因为传入的是对象
        //var avgCol: TypedColumn[UserBean, Double] = udf.toColumn.name("avgAge")
        TypedColumn<UserBean, Double> avgCol = udf.toColumn().name("avgAge");
        //读取文件
        //var frame: DataFrame = session.read.json("in/user.json")
        Dataset<Row> frame = session.read().json("in/user.json");

        //转换位dataset,DSL风格
        //var userDS: Dataset[UserBean] = frame.as[UserBean]
        Dataset<UserBean> userDS = frame.as(Encoders.bean(UserBean.class));

        //应用函数，因为传入的是对象，并且每条进行处理
        userDS.select(avgCol).show();
        
        // 释放资源     
        session.stop();        
    }
}
