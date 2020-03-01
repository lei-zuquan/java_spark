package com.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:08 上午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: flatMap算子，作用：类似于map，但是每一个输入元素可以被映射为0或多个输出元素（所以func应该返回一个序列，而不是单一元素）
 */
public class Java_Spark05_Oper4_flatMap {
    public static void main(String[] args) {
        // testFlatMap
        testFlatMapForString();
    }

    public static void testFlatMap() {
        //var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("WordCount");

        //创建Spark上下文对象
        //var sc: SparkContext = new SparkContext(config)
        JavaSparkContext sc = new JavaSparkContext(config);

        //map算子
        //var listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))
        List<int[]> list = new ArrayList<>();
        list.add(new int[]{1, 2});
        list.add(new int[]{3, 4});

        JavaRDD<int[]> listRDD = sc.parallelize(list);

        //flatMap
        //1,2,3,4
        //var flatMapRDD: RDD[Int] = listRDD.flatMap(datas => datas)
        JavaRDD<Integer> flatMapRDD = listRDD.flatMap(datas -> Arrays.stream(datas).iterator());

        //flatMapRDD.collect().foreach(println)
        flatMapRDD.collect().forEach(System.out::println);
    }

    public static void testFlatMapForString() {
        //var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("WordCount");

        //创建Spark上下文对象
        //var sc: SparkContext = new SparkContext(config)
        JavaSparkContext sc = new JavaSparkContext(config);

        //map算子
        //var listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"hello, you", "hello, me"});
        list.add(new String[]{"hello, world", "hello, spark"});

        JavaRDD<String[]> listRDD = sc.parallelize(list);

        //flatMap
        //1,2,3,4
        //var flatMapRDD: RDD[Int] = listRDD.flatMap(datas => datas)
        JavaRDD<String> flatMapRDD = listRDD.flatMap(datas -> Arrays.stream(datas).iterator());

        //flatMapRDD.collect().foreach(println)
        flatMapRDD.collect().forEach(System.out::println);
    }
}
