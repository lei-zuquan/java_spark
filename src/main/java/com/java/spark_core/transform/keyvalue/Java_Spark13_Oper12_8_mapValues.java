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
 * @Date: Created in 8:02 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: mapValues算子 作用：针对于(K,V)形式的类型只对V进行操作
 */
public class Java_Spark13_Oper12_8_mapValues {
    public static void main(String[] args) {
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("mapValues");
        JavaSparkContext sc = new JavaSparkContext(config);

        /**
         * 需求：创建一个pairRDD，并将value添加字符串"|||"
         */

        // 创建一个pairRDD
        //val rdd3 = sc.parallelize(Array((1, "a"), (1, "d"), (2, "b"), (3, "c")))
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        list.add(new Tuple2<Integer, String>(1, "a"));
        list.add(new Tuple2<Integer, String>(1, "d"));
        list.add(new Tuple2<Integer, String>(2, "b"));
        list.add(new Tuple2<Integer, String>(3, "c"));
        JavaPairRDD<Integer, String> rdd3 = sc.parallelizePairs(list);

        // 对value添加字符串"|||"
        // rdd3.mapValues(_ + "|||").collect().foreach(println)
        rdd3.mapValues(value -> value + "|||").collect().forEach(System.out::println);
    }
}
