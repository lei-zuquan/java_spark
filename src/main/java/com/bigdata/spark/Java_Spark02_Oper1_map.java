package com.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:47 上午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class Java_Spark02_Oper1_map {
    public static void main(String[] args) {
        // var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("map");

        //创建Spark上下文对象
        // var sc: SparkContext = new SparkContext(config)
        JavaSparkContext sc = new JavaSparkContext(config);

        //map算子
        // var listRDD: RDD[Int] = sc.makeRDD(1 to 10) //这里的to 是包含  10的， unto 是不包含10 的
        JavaRDD<Integer> listRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        //所有RDD里的算子都是由Execuator进行执行
        // val mapRDD:RDD[Int] = listRDD.map(_*2)
        JavaRDD<Integer> mapRDD = listRDD.map(t -> t * 2);

        // mapRDD.collect().foreach(println)
        mapRDD.collect().forEach(System.out::println);
    }
}
