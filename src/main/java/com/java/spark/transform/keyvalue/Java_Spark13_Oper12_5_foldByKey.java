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
 * @Date: Created in 5:38 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: foldByKey算子 作用：aggregateByKey的简化操作，seqop和combop相同
 */
public class Java_Spark13_Oper12_5_foldByKey {
    public static void main(String[] args) {
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("foldByKey");
        JavaSparkContext sc = new JavaSparkContext(config);

        // val rdd = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)
        List<Tuple2<Integer, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>(1, 3));
        list.add(new Tuple2<>(1, 2));
        list.add(new Tuple2<>(1, 4));
        list.add(new Tuple2<>(2, 3));
        list.add(new Tuple2<>(3, 6));
        list.add(new Tuple2<>(3, 8));
        JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(list, 3);
        /**
         * 创建一个pairRDD，计算相同key对应值的相加结果
         */
        // val agg = rdd.foldByKey(0)(_ + _)
        JavaPairRDD<Integer, Integer> agg = rdd.foldByKey(0, (v1, v2) -> v1 + v2);

        // agg.collect().foreach(println)
        agg.collect().forEach(System.out::println);
    }
}
