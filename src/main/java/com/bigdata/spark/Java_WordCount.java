package com.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:47 上午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class Java_WordCount {
    public static void main(String[] args) {
        //使用idea工具开发
        //local 模式

        // 创建SparkConf()对象
        //设置Spark计算框架的运行（部署）环境
        //val config : SparkConf = new SparkConf().setMaster("yarn").setAppName("WordCount")//local[*]
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("WordCount");//local[*]

        //创建spark 上下文对象
        JavaSparkContext sc = new JavaSparkContext(config);

        //读取文件,将文件内容一行一行的读取出来
        //路径查找位置默认从当前的部署环境中查找
        /**
         JavaPairRDD<String, Integer> counts = sc.textFile("in/wordcount.txt")
         .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
         .mapToPair(word -> new Tuple2<>(word, 1))
         .reduceByKey((a, b) -> a + b);
         */

        JavaRDD<String> lines = sc.textFile("in/wordcount.txt");

        //将一行行的数据分解一个一个的单词
        JavaRDD<String> words = lines.flatMap(t -> Arrays.asList(t.split(" ")).iterator());

        //RDD是 把数据处理的逻辑进行了封装
        //为了统计方便，将单词进行结构的转换
        JavaPairRDD<String, Integer> wordToOne = words.mapToPair(word -> new Tuple2<>(word, 1));

        //将转换结构后的数据进行分组聚合
        JavaPairRDD<String, Integer> wordToSum = wordToOne.reduceByKey((a, b) -> a + b);

        //将统计的结果打印到控制台
        List<Tuple2<String, Integer>> result = wordToSum.collect();

        System.out.println(result);
        for (Tuple2<String, Integer> tuple : result) {
            System.out.println(tuple._1 + " appear:" + tuple._2);
        }
    }

}
