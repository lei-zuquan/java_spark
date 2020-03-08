package com.java.spark_core.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 3:19 下午 2020/3/2
 * @Version: 1.0
 * @Modified By:
 * @Description: RDD中的函数传递
 */

/**
 * 在实际开发中我们往往需要自己定义一些对于RDD的操作，那么此时需要主要的是，初始化工作是在Driver端进行的，
 * 而实际运行程序是在Executor端进行的，这就涉及到了跨进程通信，是需要序列化的。
 * <p>
 * Caused by: java.io.NotSerializableException: com.scala.spark.Search
 * 4．问题说明
 * //过滤出包含字符串的RDD
 * def getMatch1 (rdd: RDD[String]): RDD[String] = {
 * rdd.filter(isMatch)
 * }
 * 在这个方法中所调用的方法isMatch()是定义在Search这个类中的，实际上调用的是this. isMatch()，this表示Search这个类的对象，程序在运行过程中需要将Search对象序列化以后传递到Executor端。
 * 5．解决方案
 * 使类继承scala.Serializable即可。
 * class Search() extends Serializable{...}
 */
public class Java_Spark15_Serializable {
    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("Java_Spark15_Serializable");
        JavaSparkContext sc = new JavaSparkContext(config);

        //val rdd:RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "songdongodng"))
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hadoop", "spark", "hive", "songdongodng"));

        //val search = new Search("h")
        Search search = new Search("h");

        //    val match1:RDD[String] = search.getMatch1(rdd)
        //JavaRDD<String> match1 = search.getMatch1(rdd);

        //val match1:RDD[String] = search.getMatch2(rdd)
        JavaRDD<String> match1 = search.getMatch2(rdd);


        //match1.collect().foreach(println)
        match1.collect().forEach(System.out::println);

        //释放资源
        sc.stop();
    }
}


//query

class Search implements java.io.Serializable {
    //class Search {
    private String query;


    public Search(String query) {
        this.query = query;
    }

    //过滤出包含字符串的数据
    public boolean isMatch(String s) {
        return s.contains(query);
    }

    //过滤出包含字符串的RDD
    public JavaRDD<String> getMatch1(JavaRDD<String> rdd) {
        return rdd.filter(t -> isMatch(t)); //在这个方法中所调用的方法isMatch()是定义在Search这个类中的，实际上调用的是this. isMatch()，this表示Search这个类的对象，程序在运行过程中需要将Search对象序列化以后传递到Executor端
    }

    //过滤出包含字符串的RDD
    public JavaRDD<String> getMatch2(JavaRDD<String> rdd) {
        String q = query; //成员属性，字符串本身就会序列化，将类变量赋值给局部变量
        //rdd.filter(x => x.contains(query))
        //return rdd.filter(x -> x.contains(q));
        return rdd.filter(x -> x.contains(query));
    }

}
