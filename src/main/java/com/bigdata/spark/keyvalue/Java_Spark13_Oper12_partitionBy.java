package com.bigdata.spark.keyvalue;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 2:50 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: partitionBy算子 作用：对pairRDD进行分区操作，如果原有的partionRDD和现有的partionRDD是一致的话就不进行分区， 否则会生成ShuffleRDD，即会产生shuffle过程。
 */
public class Java_Spark13_Oper12_partitionBy {
    public static void main(String[] args) {
        // var config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("partitionBy")
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("partitionBy");

        // 创建Spark上下文对象
        // var sc:SparkContext = new SparkContext(config)
        JavaSparkContext sc = new JavaSparkContext(config);
        // map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。

        // 从指定
        //    var listRDD: RDD[Int] = sc.makeRDD( 1 to 16) //这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数
        // var listRDD:RDD[(String, Int)] =sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3),("c", 2),("d", 4)))
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>("a", 1));
        list.add(new Tuple2<>("b", 2));
        list.add(new Tuple2<>("c", 3));
        list.add(new Tuple2<>("d", 4));
        JavaRDD<Tuple2<String, Integer>> listRDD = sc.parallelize(list);
        JavaPairRDD<String, Integer> pairRDD = listRDD.mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2));

        // var listRDD2:RDD[(String, Int)] =sc.makeRDD(List(("a", 4), ("b", 6), ("c", 8),("c", 0),("d", 22)))
        List<Tuple2<String, Integer>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>("a", 4));
        list2.add(new Tuple2<>("b", 6));
        list2.add(new Tuple2<>("c", 8));
        list2.add(new Tuple2<>("c", 0));
        list2.add(new Tuple2<>("d", 22));
        JavaRDD<Tuple2<String, Integer>> listRDD2 = sc.parallelize(list2);
        JavaPairRDD<String, Integer> pairRDD2 = listRDD2.mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2));
        // 自定义分区
        // var partRDD: RDD[(String, Int)] = listRDD.partitionBy(new MyPartitioner(3))
        //JavaPairRDD<String, Integer> partRDD = pairRDD.partitionBy(new MyJavaPartitioner(3));
        //JavaPairRDD<String, Integer> partRDD2 = pairRDD2.partitionBy(new MyJavaPartitioner(3));

        // 一般是为了传入一个分区器 new org.apache.spark.HashParititon(2)
        // var partRDD:RDD[(String, Int)] = listRDD.partitionBy(new org.apache.spark.HashPartitioner(2))
        JavaPairRDD<String, Integer> partRDD = pairRDD2.partitionBy(new org.apache.spark.HashPartitioner(2));

        partRDD.saveAsTextFile("output");
        //partRDD2.saveAsTextFile("output");
    }
}


//声明分区器
class MyJavaPartitioner extends Partitioner {
    private int partitions;

    public MyJavaPartitioner(int partitions) {
        this.partitions = partitions;
    }

    @Override
    public int numPartitions() {
        return partitions;
    }

    @Override
    public int getPartition(Object key) {
        return 1;
    }
}