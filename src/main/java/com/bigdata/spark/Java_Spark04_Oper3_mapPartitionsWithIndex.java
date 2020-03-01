package com.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:27 上午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description:   mapPartitionsWithIndex：带有一个整数参数表示分片的索引值
 */
public class Java_Spark04_Oper3_mapPartitionsWithIndex {
    public static void main(String[] args) {
        // var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("WordCount");

        // 创建Spark上下文对象
        //var sc: SparkContext = new SparkContext(config)
        JavaSparkContext sc = new JavaSparkContext(config);

        // map算子
        //var listRDD: RDD[Int] = sc.makeRDD(1 to 10 ,2) //这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        JavaRDD<Integer> listRDD = sc.parallelize(list, 2);

        // 方法 使用大括号，是启用模式匹配的效果,//num 是分区号，
        //    var indexRDD: RDD[Int] = listRDD.mapPartitionsWithIndex {
        //      case (num, datas) => {
        //        datas
        //      }
        //    }

        // var tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
        //    case (num, datas) => {
        //        datas.map((_, "分区号：" + num))
        //    }
        //}
        JavaRDD<String> tupleRDD = listRDD.mapPartitionsWithIndex((Integer v1, Iterator<Integer> iterator) -> {
            List<String> returnList = new ArrayList<>();
            while (iterator.hasNext()) {
                Integer next = iterator.next();
                returnList.add(next + " 在：" + v1 + "号分区");
            }
            return returnList.iterator();
        }, true); // preservesPartitioning，是否保留父RDD的partitioner分区信息

        //tupleRDD.collect().foreach(println)
        tupleRDD.collect().forEach(System.out::println);
    }
}
