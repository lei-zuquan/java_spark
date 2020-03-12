package com.scala.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: Lei
  * @E-mail: 843291011@qq.com
  * @Date: 2020-03-12 9:51
  * @Version: 1.0
  * @Modified By:
  * @Description:
  */

/*
  pom.xml文件添加依赖：
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming-kafka-0-8_2.11</artifactId>
    <version>2.1.1</version>
  </dependency>
  <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
      <version>0.11.0.2</version>
  </dependency>




  在工程中需要引入 Maven 工件 spark- streaming-kafka_2.10 来使用它。包内提供的 KafkaUtils 对象可以
  在 StreamingContext 和 JavaStreamingContext 中以你的 Kafka 消息创建出 DStream。由于 KafkaUtils
   可以订阅多个主题，因此它创建出的 DStream 由成对的主题和消息组成。要创建出一个流数据，需要使用
   StreamingContext 实例、一个由逗号隔开的 ZooKeeper 主机列表字符串、消费者组的名字(唯一名字)，以及一个
   从主题到针对这个主题的接收器线程数的映射表来调用 createStream() 方法。

 */
object SparkStreaming04_KafkaSource {
  def main(args: Array[String]): Unit = {
    //使用SparkStreaming 完成WordCount

    //SparkSQL
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")

    var streamingContext: StreamingContext = new StreamingContext(config, Seconds(3)) //3 秒钟，伴生对象，不需要new

    //从kafka中采集数据
    var kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext, "linux1:2181", "xiaodong", Map("xiaodong" -> 3))

    //将采集的数据进行分解（偏平化）
    var WordDstream: DStream[String] = kafkaDStream.flatMap(t => t._2.split(" ")) //偏平化后，按照空格分割

    //将我们的数据进行转换方便分析
    var mapDstream: DStream[(String, Int)] = WordDstream.map((_, 1))

    //将转换后的数据聚合在一起处理
    var wordToSumStream: DStream[(String, Int)] = mapDstream.reduceByKey(_ + _)

    //打印结果
    wordToSumStream.print()

    //streamingContext.stop()  //不能停止我们的采集功能

    //启动采集器
    streamingContext.start()

    //Driver等待采集器停止，
    streamingContext.awaitTermination()
  }
}
