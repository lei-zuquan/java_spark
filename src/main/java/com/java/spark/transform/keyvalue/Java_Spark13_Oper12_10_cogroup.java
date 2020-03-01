package com.java.spark.transform.keyvalue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 8:49 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: cogroup算子(类似leftOutJoin) 作用：在类型为(K,V)和(K,W)的RDD上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的RDD
 */
public class Java_Spark13_Oper12_10_cogroup {
    public static void main(String[] args) {
        // 创建JavaSparkContext上下文对象
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("cogroup");
        JavaSparkContext sc = new JavaSparkContext(config);

        /**
         * 需求：创建两个pairRDD，并将key相同的数据聚合到一个迭代器。
         */
        // 创建第一个pairRDD
        // val rdd = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c"), (4, "d")))
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        list.add(new Tuple2<>(1, "a"));
        list.add(new Tuple2<>(2, "b"));
        list.add(new Tuple2<>(3, "c"));
        list.add(new Tuple2<>(4, "d"));
        JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(list);

        // 创建第二个pairRDD
        // val rdd1 = sc.parallelize(Array((1, 4), (2, 5), (3, 6), (5, 5)))
        List<Tuple2<Integer, Integer>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(1, 4));
        list1.add(new Tuple2<>(2, 5));
        list1.add(new Tuple2<>(3, 6));
        list1.add(new Tuple2<>(5, 5));
        JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(list1);

        // cogroup两个RDD并打印结果
        // rdd.cogroup(rdd1).collect().foreach(println)
        rdd.cogroup(rdd1).collect().forEach(System.out::println);
    }
}
