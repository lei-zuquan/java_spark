package com.java.spark_sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
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
    //private static Object User;

    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("SparkSQL03_transform");
        JavaSparkContext sc = new JavaSparkContext(config);


        //SparkSession session = new SparkSession(sc.sc()); // 方法私有，不能正常创建
        SparkSession session = SparkSession.builder().config(config).getOrCreate();

        // 创建RDD
        // var rdd: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List((1, "zhagnshan", 10), (2, "lisi", 20)))
        List<User> list = new ArrayList<>();
        list.add(new User(1, "zhangsan", 10));
        list.add(new User(2, "lisi", 20));
        list.add(new User(3, "wangwu", 30));
        JavaRDD<User> rdd = sc.parallelize(list, 2);

        // 进行转换前，需要引入隐式转换规则。
        // 这里spark_session不是包的名字，是SparkSession的对象
        //import session.implicits._;

        // 转换为DF
        //var df:DataFrame = rdd.toDF("id", "name", "age")
        JavaRDD<Row> studentRDD = rdd.map(user -> {
            return RowFactory.create(user.getId(), user.getName(), user.getAge());
        });
        // 第二步，动态构造元数据
        // 比如说，id、name等，field的名称和类型，可能都是在程序运行过程中，动态从mysql db里
        // 或者是配置文件中，加载出来的，是不固定的
        // 所以特别适合用这种编程的方式，来构造元数据
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);


        // 第三步，将使用动态构造的元数据，将RDD转换为DataFrame
        /**
         * 特别说明：在java版本中没有DataFrame类型，代替的是： Dataset<Row>
         *     在spark 2.x版本中 认为dataframe是 Dataset的形式：Dataset<Row>
         *
         * 如果同样的数据都给到这三个数据结构，他们分别计算之后，都会给出相同的结果。不同是的他们的执行效率和执行方式。
         * 在后期的Spark版本中，DataSet会逐步取代RDD和DataFrame成为唯一的API接口。
         */
        // 转换为DF
        // var df: DataFrame = rdd.toDF("id", "name", "age")
        // 创建方式一：RDD (Spark1.0) —> Dataframe(Spark1.3) —> Dataset(Spark1.6)
        Dataset<Row> old1_6_df = session.createDataFrame(studentRDD, structType);
        old1_6_df.show();

        // 创建方式二：推荐使用这种方式
        Dataset<Row> df = session.createDataFrame(list, User.class);
        df.show();

        // 转换为DS
        // var ds: Dataset[User] = df.as[User] //这里需要创建样例类

        Encoder<User> userEncoder = Encoders.bean(User.class);
        Dataset<User> ds = df.as(userEncoder);
        System.out.println("=============================================");
        ds.show();
        System.out.println("=============================================");

        // 转换为DF
        // var df1:DataFrame = ds.toDF()
        Dataset<Row> df1 = ds.toDF();

        // 转换为RDD
        /*
        var rdd1:RDD[Row] = df1.rdd //这里是 row 类型需要注意
        rdd1.foreach(row -> {
            // 获取数据时，可以通过索引访问数据
            System.out.print(row.getInt(0) + "\t");
            System.out.print(row.getString(1) + "\t");
            // System.out.println(row.getInt(2) + "\t");
            return null;
        }); */
        JavaRDD<Row> rdd1 = df1.javaRDD();
        rdd1.foreach( row -> {
            String userInfo = row.getInt(0) + "\t" + row.getInt(1) + "\t" + row.getString(2) + "\t";
            System.out.println(userInfo);
        });

        // 释放资源
        session.stop();
    }
}


