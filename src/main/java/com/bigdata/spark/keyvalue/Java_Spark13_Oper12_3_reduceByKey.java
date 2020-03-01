package com.bigdata.spark.keyvalue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 3:59 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: reduceByKey算子 作用：在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起，reduce任务的个数可以通过第二个可选的参数来设置
 */

/**
 * reduceByKey和groupByKey的区别
 * 1. reduceByKey：按照key进行聚合，在shuffle之前有combine（本地预聚合）操作，返回结果是RDD[k,v].
 * 2. groupByKey：按照key进行分组，直接进行shuffle。
 * 3. 开发指导：reduceByKey比groupByKey，建议使用。但是需要注意是否会影响业务逻辑。
 */
public class Java_Spark13_Oper12_3_reduceByKey {
    public static void main(String[] args) {
        //var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("reduceByKey");

        // 创建Spark上下文对象
        //var sc: SparkContext = new SparkContext(config)
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("reduceByKey");
        JavaSparkContext sc = new JavaSparkContext(config);


        //val rdd = sc.parallelize(List(("female", 1), ("male", 5), ("female", 5), ("male", 2)))
        List<Tuple2<String, Integer>> list = new ArrayList();
        list.add(new Tuple2<String, Integer>("female", 1));
        list.add(new Tuple2<String, Integer>("male", 5));
        list.add(new Tuple2<String, Integer>("female", 5));
        list.add(new Tuple2<String, Integer>("male", 2));
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);

        //val reduce = rdd.reduceByKey((x, y) => x + y)
        JavaPairRDD<String, Integer> reduce = rdd.reduceByKey((a, b) -> a + b);

        //reduce.collect().foreach(println)
        rdd.collect().forEach(System.out::println);
    }
}
