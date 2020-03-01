package com.java.spark.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:32 上午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: glom算子 作用：将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
 */
public class Java_Spark06_Oper5_glom {

    public static void main(String[] args) {
        // var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("glom");

        // 创建Spark上下文对象
        // var sc: SparkContext = new SparkContext(config)
        JavaSparkContext sc = new JavaSparkContext(config);

        // map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。
        // var listRDD: RDD[Int] = sc.makeRDD(1 to 16, 3) //这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数
        List list = new ArrayList();
        for (int i = 1; i <= 16; i++) {
            list.add(i);
        }
        JavaRDD listRDD = sc.parallelize(list, 3);

        // 将一个分区的数据，放到数组中
        // var glomRDD: RDD[Array[Int]] = listRDD.glom()
        JavaRDD glomRDD = listRDD.glom();

        // glomRDD.collect().foreach(println)
        // glomRDD.collect().foreach(array => {
        //         println(array.mkString(","))
        // })
        glomRDD.collect().forEach(System.out::println);

        sc.close();
    }
}
