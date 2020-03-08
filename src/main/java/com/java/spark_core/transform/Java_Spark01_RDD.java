package com.java.spark_core.transform;

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
public class Java_Spark01_RDD {
    public static void main(String[] args) {
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("WordCount");
        //1,创建上下文对象
        JavaSparkContext sc = new JavaSparkContext(config);
        //创建RDD
        //     1)从内存中长假RDD，底层就是实现parallelize
        //var listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
        JavaRDD<Integer> listRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        //使用自定义分区: JAVA版本无makeRDD算子
        //var listRDD_s: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
        JavaRDD<Integer> listRDD_s = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6), 2);

        //2) 从内存中创建parallelize
        //var arrayRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))
        //arrayRDD.collect().foreach(println)
        JavaRDD<Integer> arrayRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        //listRDD.collect().forEach(System.out::println);

        //3) 从外部存储中创建
        //默认情况下，可以读取项目路径，也可以读取hdfs
        //默认从文件读取的数据，都是字符类型
        //读取文件时，传递的参数为最小分区数，但是不一定是这个分区数，取决于hadoop读取文件的分片规则
        //var fileRDD: RDD[String] = sc.textFile("in",3)
        JavaRDD<String> fileRDD = sc.textFile("in", 3);

        //将RDD的数据保存到文件中
        //fileRDD.saveAsTextFile("output") //默认 电脑核数
        fileRDD.saveAsTextFile("output"); //默认 电脑核数
    }
}
