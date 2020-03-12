package com.java.spark_streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-12 9:35
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class Java_SparkStreaming02_FileDataSource {
    public static void main(String[] args){
        // 使用SparkStreaming
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming02_FileDataSource");
        JavaStreamingContext streamingContext = new JavaStreamingContext(config, Seconds.apply(3)); //3 秒钟，伴生对象，不需要new

        // 从文件夹中采集数据
        JavaDStream<String> fileDStreaming = streamingContext.textFileStream("test");

        // 将采集的数据进行分解（偏平化）
        JavaDStream<String> wordDstream = fileDStreaming.flatMap(
                (String line) -> Arrays.asList(line.split(" ")).iterator() //偏平化后，按照空格分割
        );

        // 将我们的数据进行转换方便分析
        JavaDStream<Tuple2<String, Integer>> mapDstream = wordDstream.map((String word) -> new Tuple2<String, Integer>(word, 1));

        // 将转换后的数据聚合在一起处理
        JavaDStream<Tuple2<String, Integer>> wordToSumStream = mapDstream.reduce((t1, t2) -> new Tuple2(t1._1, t1._2 + t2._2));

        // 打印结果
        wordToSumStream.print();

        // 启动采集器
        streamingContext.start();

        // Driver 等待采集器停止，
        try {
            streamingContext.awaitTermination();
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
