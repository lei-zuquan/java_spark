package com.java.spark.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 1:37 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: coalesce算子 作用：缩减分区数，用于大数据集过滤后，提高小数据集的执行效率。
 */
public class Java_Spark11_Oper10_coalesce {
    public static void main(String[] args) {
        // var config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("coalesce");

        // 创建Spark上下文对象
        // var sc:SparkContext = new SparkContext(config)
        JavaSparkContext sc = new JavaSparkContext(config);

        // map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。
        // 从指定
        // var listRDD:RDD[Int] = sc.makeRDD(1to 16)
        // println("缩减分区前 = " + listRDD.partitions.size)
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
            list.add(i);
        }
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        System.out.println("缩减分区前 = " + listRDD.getNumPartitions());

        // 缩减分区，可以简单的理解为合并分区，没有shuffle，会把剩余的数据存储在最后
        // var coalesceRDD:RDD[Int] = listRDD.coalesce(3)
        JavaRDD<Integer> coalesceRDD = listRDD.coalesce(3);

        // println("缩减分区后 = " + coalesceRDD.partitions.size)
        System.out.println("缩减分区后 = " + coalesceRDD.getNumPartitions());
    }
}
