package com.java.spark_core.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 12:28 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: distinct算子 作用：对源RDD进行去重后返回一个新的RDD。默认情况下，只有8个并行任务来操作，但是可以传入一个可选的numTasks参数改变它。
 */
public class Java_Spark10_Oper9_distinct {
    public static void main(String[] args) {
        // var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("distinct");

        // 创建Spark上下文对象

        // var sc: SparkContext = new SparkContext(config)
        JavaSparkContext sc = new JavaSparkContext(config);
        // map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。

        // 从指定
        // var listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 2, 23, 3, 3, 4, 4, 4, 4, 6, 7, 7, 0)) //这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数
        List<Integer> list = Arrays.asList(1, 2, 2, 23, 3, 3, 4, 4, 4, 4, 4, 6, 7, 7, 0);
        JavaRDD<Integer> listRDD = sc.parallelize(list);

        // distinct  对数据进行去重，但是因为它会导致数据去重后减少，所以可以改变默认的分区数量
        // 一个分区，就是一个任务（task)。一个任务，会分配到一个execuator
        // var distinctRDD: RDD[Int] = listRDD.distinct(2)
        JavaRDD<Integer> distinctRDD = listRDD.distinct(2);

        // distinctRDD.collect().foreach(println//控制台打印，没有保存之前的顺序    401622337, 数据打乱重组，shuffle
        distinctRDD.collect().forEach(System.out::println);

        //distinctRDD.saveAsTextFile("output") //数据 进行了shuffle 打乱重组，没有保存之前的顺序，每个分区存的数据和之前不一样了。
        // 在spark中所有转换算子中，没有shuffle则速度快。
    }
}
