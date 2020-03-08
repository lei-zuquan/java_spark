package com.java.spark_core.practice;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:02 上午 2020/3/2
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class Practice {
    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[1]").setAppName("Practice");
        JavaSparkContext sc = new JavaSparkContext(config);

        /**
         * 1. 数据结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割。
         * 2. 需求：统计出每一个省份广告被点击次数的TOP3
         * 1516609143867 6 7 64 16
         * 1516609143869 9 4 75 18
         * 1516609143869 1 7 87 12
         */
        //2.读取数据生成RDD：TS，Province，City，User，AD
        //val line = sc.textFile("in/agent.log")
        JavaRDD<String> line = sc.textFile("in/agent.log");

        //3.按照最小粒度聚合：((Province,AD),1)
        // val provinceAdToOne = line.map {
        //     x =>
        //     val fields:Array[String] = x.split(" ")
        //     ((fields(1), fields(4)),1)
        // }
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> provinceAdToOne = line.mapToPair(record -> {
            String[] fields = record.split(" ");
            return new Tuple2<Tuple2<Integer, Integer>, Integer>(
                    new Tuple2<Integer, Integer>(Integer.valueOf(fields[1]), Integer.valueOf(fields[4])),
                    1
            );
        });


        //4.计算每个省中每个广告被点击的总数：((Province,AD),sum)
        // val provinceAdToSum = provinceAdToOne.reduceByKey(_ + _)
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> provinceAdToSum = provinceAdToOne.reduceByKey((a, b) -> a + b);

        //5.将省份作为key，广告加点击数为value：(Province,(AD,sum))
        // val provinceToAdSum = provinceAdToSum.map(x = > (x._1._1, (x._1._2, x._2)))
//        JavaRDD<Tuple2<Integer, Tuple2<Integer, Integer>>> provinceToAdSum = provinceAdToSum.map(tuple -> {
//            return new Tuple2<>(tuple._1._1,
//                    new Tuple2<>(tuple._1._2, tuple._2));
//        });

        JavaPairRDD<Integer, Tuple2<Integer, Integer>> provinceToAdSum = provinceAdToSum.mapToPair(tuple -> {
            return new Tuple2<>(tuple._1._1,
                    new Tuple2<>(tuple._1._2, tuple._2));
        });

        //6.将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))
        // val provinceGroup = provinceToAdSum.groupByKey()
        JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> provinceGroup = provinceToAdSum.groupByKey();

        //7.对同一个省份所有广告的集合进行排序并取前3条，排序规则为广告点击总数
        // val provinceAdTop3 = provinceGroup.mapValues {
        //     x =>
        //     x.toList.sortWith((x, y) =>x._2 > y._2).take(3)
        // }

        JavaPairRDD<Integer, Iterator<Tuple2<Integer, Integer>>> provinceAdTop3 = provinceGroup.mapValues(values -> {
            //return values; // 这样运行不报错，如下方式进行报错
            Iterator<Tuple2<Integer, Integer>> iterator2 = values.iterator();
            List<Tuple2<Integer, Integer>> list = new ArrayList<>();
            while (iterator2.hasNext()) {
                list.add(iterator2.next());
            }
            Iterator<Tuple2<Integer, Integer>> iterator = list.stream().sorted((t1, t2) -> -t1._2.compareTo(t2._2)).limit(3).iterator();
            return iterator;
        });

        //8.将数据拉取到Driver端并打印
        // provinceAdTop3.collect().foreach(println)
        provinceAdTop3.collect().forEach(System.out::println);

        //9.关闭与spark的连接
        // sc.stop()
        sc.stop();
    }
}
