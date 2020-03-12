package com.java.spark_streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-12 9:07
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
  添加pom.xml文件依赖
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming_2.11</artifactId>
            <version>2.1.1</version>
        </dependency>
 */

public class Java_SparkStreaming01_WordCount {
    public static void main(String[] args){
        //使用SparkStreaming 完成WordCount
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount");

        JavaStreamingContext streamingContext = new JavaStreamingContext(config, Seconds.apply(3)); //3 秒钟，伴生对象，不需要new

        //从指定的端口中采集数据
        JavaReceiverInputDStream<String> socketLineStreaming = streamingContext.socketTextStream("leizuquandeMacBook-Air.local", 9999);//一行一行的接受

        //将采集的数据进行分解（偏平化）
        JavaDStream<String> wordDStream = socketLineStreaming.flatMap( (String line) ->{
                String[] split = line.split(" ");
                return Arrays.asList(split).iterator();
        });

        //将我们的数据进行转换方便分析
        JavaDStream<Tuple2<String, Integer>> mapDstream = wordDStream.map(word -> new Tuple2<String, Integer>(word, 1));

        //将转换后的数据聚合在一起处理
        JavaDStream<Tuple2<String, Integer>> wordToSumStream = mapDstream.reduce(
                (t1, t2) -> {
                    return new Tuple2<>(t1._1, t1._2 + t2._2);
                });

        //打印结果
        wordToSumStream.print();

        //streamingContext.stop()  //不能停止我们的采集功能

        //启动采集器
        streamingContext.start();

        //Drvier等待采集器停止，
        try {
            streamingContext.awaitTermination();
        } catch (Exception ex){
            ex.printStackTrace();
        }


        //nc -lc 9999   linux 下往9999端口发数据。
    }
}
