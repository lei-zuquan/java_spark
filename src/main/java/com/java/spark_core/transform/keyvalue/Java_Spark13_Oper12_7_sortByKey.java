package com.java.spark_core.transform.keyvalue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:51 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: sortByKey算子 作用：在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的(K,V)的RDD
 */
public class Java_Spark13_Oper12_7_sortByKey {
    public static void main(String[] args) {
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("sortByKey");
        JavaSparkContext sc = new JavaSparkContext(config);


        /**
         * 需求：创建一个pairRDD，按照key的正序和倒序进行排序
         */
        // val rdd = sc.parallelize(Array((3, "aa"), (6, "cc"), (2, "bb"), (1, "dd")))
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        list.add(new Tuple2<>(3, "aa"));
        list.add(new Tuple2<>(6, "cc"));
        list.add(new Tuple2<>(2, "bb"));
        list.add(new Tuple2<>(1, "dd"));
        JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(list);

        // 按照key的正序
        // rdd.sortByKey(true).collect().foreach(println)
        rdd.sortByKey(true).collect().forEach(System.out::println);

        System.out.println("--------上面是按key的正序，下面是按key的倒序-------------------");
        // 按照key的倒序
        rdd.sortByKey(false).collect().forEach(System.out::println);
    }
}
