package com.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:43 上午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: groupBy算子 作用：分组，按照传入函数的返回值进行分组。将相同的key对应的值放入一个迭代器。
 */
public class Java_Spark07_Oper6_groupBy {
    public static void main(String[] args) {
        // var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupBy")
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("groupBy");

        // 创建Spark上下文对象
        // var sc: SparkContext = new SparkContext(config)
        JavaSparkContext sc = new JavaSparkContext(config);

        // map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。
        // 生成数据，并按指定规则进行分组
        // 分组后的数据，形成对偶元组（k-v),k表示分组的key  , value 表示分组的集合
        // var listRDD: RDD[Int] = sc.makeRDD(1 to 16) // 这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数
        List<Integer> list = new ArrayList();
        for (int i = 1; i <= 16; i++) {
            list.add(i);
        }
        JavaRDD<Integer> listRDD = sc.parallelize(list, 2);

        // var groupbyRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i => i % 2)
        JavaPairRDD groupByRDD = listRDD.groupBy(i -> i % 2);

        // groupbyRDD.collect().foreach(println)
        groupByRDD.collect().forEach(System.out::println);

        sc.close();
    }
}
