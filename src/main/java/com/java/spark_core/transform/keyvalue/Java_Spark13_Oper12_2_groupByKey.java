package com.java.spark_core.transform.keyvalue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 3:40 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: groupByKey算子 作用：groupByKey也是对每个key进行操作，但只生成一个sequence。
 */
public class Java_Spark13_Oper12_2_groupByKey {
    public static void main(String[] args) {
        //var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByKey")
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("groupByKey");

        // 创建Spark上下文对象
        //var sc: SparkContext = new SparkContext(config)
        JavaSparkContext sc = new JavaSparkContext(config);

        //val words = Array("one", "two", "two", "three", "three", "three")
        List<String> words = Arrays.asList("one", "two", "two", "three", "three", "three");

        //val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))
        JavaPairRDD<String, Integer> wordPairsRDD = sc.parallelize(words).mapToPair(word -> new Tuple2<>(word, 1));

        //val group = wordPairsRDD.groupByKey()
        JavaPairRDD<String, Iterable<Integer>> group = wordPairsRDD.groupByKey();

        //group.collect().foreach(println)
        group.collect().forEach(System.out::println);

        //group.map(t => (t._1, t._2.sum))

        //group.map(t => (t._1, t._2.sum))
    }
}
