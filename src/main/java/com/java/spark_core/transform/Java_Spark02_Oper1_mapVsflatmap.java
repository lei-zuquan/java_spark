package com.java.spark_core.transform;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
public class Java_Spark02_Oper1_mapVsflatmap {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);
        // var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("map");

        // 创建 Spark上下文对象
        // var sc: SparkContext = new SparkContext(config)
        JavaSparkContext sc = new JavaSparkContext(config);


        JavaRDD<String> listRDD = sc.parallelize(Arrays.asList("hello world", "hello spark"));

        // 所有RDD里的算子都是由 Executor 进行执行
        // map 算子
        System.out.println("=【map】操作 及 map操作结果打印 =======================");
        JavaRDD<String> mapRDD = listRDD.map(t -> t + "_1");
        mapRDD.collect().forEach(System.out::println);

        // flatMap 算子
        System.out.println("=【flatMap】操作 及 flatMap操作结果打印 =======================");
        JavaRDD<String> wordRdd = listRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        wordRdd.collect().forEach(System.out::println);

        sc.close();
    }
}
