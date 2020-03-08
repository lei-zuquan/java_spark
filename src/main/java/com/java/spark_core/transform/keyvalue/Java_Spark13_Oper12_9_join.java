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
 * @Date: Created in 8:25 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: join算子(性能较低) 作用：在类型为(K,V)和(K,W)的RDD上调用，返回一个相同key对应的所有元素对在一起的(K,(V,W))的RDD
 */
public class Java_Spark13_Oper12_9_join {
    public static void main(String[] args) {
        // 创建JavaSparkContext上下文对象
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("join");
        JavaSparkContext sc = new JavaSparkContext(config);

        /**
         * 需求：创建两个pairRDD，并将key相同的数据聚合到一个元组。
         */
        // 创建第一个pairRDD
        // val rdd = sc.parallelize(Array((1, "a"), (2, "b"), (3, "c")))
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        list.add(new Tuple2<>(1, "a"));
        list.add(new Tuple2<>(2, "b"));
        list.add(new Tuple2<>(3, "c"));
        JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(list);

        // 创建第二个pairRDD
        // val rdd1 = sc.parallelize(Array((1, 4), (2, 5), (3, 6)))
        List<Tuple2<Integer, Integer>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>(1, 4));
        list2.add(new Tuple2<>(2, 5));
        list2.add(new Tuple2<>(3, 6));
        list2.add(new Tuple2<>(4, 7));
        JavaPairRDD<Integer, Integer> rdd1 = sc.parallelizePairs(list2);

        // join操作并打印结果
        // rdd.join(rdd1).collect().foreach(println)
        rdd.join(rdd1).collect().forEach(System.out::println);
    }
}
