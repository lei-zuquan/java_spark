package com.scala.spark_streaming


import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-11 12:56
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
  前期准备：

  创建topic
  kafka-topics --create --zookeeper node-01:2181,node-02:2181,node-03:2181 --replication-factor 2 --partitions 1 --topic directKafka_test

  创建生产者
  kafka-console-producer --broker-list node-01:9092,node-02:9092,node-03:9092 --topic directKafka_test

  创建消费者
  kafka-console-consumer --bootstrap-server node-01:9092,node-02:9092,node-03:9092 --topic directKafka_test --group directKafka_test_g1 --from-beginning

  查看topic
  kafka-topics --zookeeper node-01:2181,node-02:2181,node-03:2181 --topic directKafka_test --describe

  增加分区
  kafka-topics --zookeeper node-01:2181,node-02:2181,node-03:2181 --alter --topic directKafka_test --partitions 2

  查看kafka偏移量
  kafka-consumer-groups --bootstrap-server node-01:9092,node-02:9092,node-03:9092 --describe --group directKafka_test_g1

  # 列出所有消费者的情况
  kafka-consumer-groups --bootstrap-server node-01:9092,node-02:9092,node-03:9092 --list

  删除消费者组
  kafka-consumer-groups --bootstrap-server node-01:9092,node-02:9092,node-03:9092 --delete --group directKafka_test_g1

  删除topic
  kafka-topics --delete --zookeeper node-01:2181,node-02:2181,node-03:2181 --topic directKafka_test

  # 查看kafka主题列表
  kafka-topics --list --zookeeper node-01:2181,node-02:2181,node-03:2181
 */

object SparkStreaming04_DirectKafkaWordCount {
  def main(args: Array[String]): Unit = {
    /*
      node-01:9092,node-02:9092,node-03:9092 directKafka_test

     */
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    var conf = new SparkConf()
      .setAppName("DirectKafka")
      .setMaster("local[2]")
      // .set("spark.streaming.receiver.maxRate", "1") // receiver方式接收; 限制每个 receiver 每秒最大可以接收的记录的数据
      .set("spark.streaming.kafka.maxRatePerPartition", "2000") // 限流，对目标topic每个partition每秒钟拉取的数据条数 条数/（每个partition）
      .set("spark.streaming.backpressure.enabled", "true") // 启用backpressure机制，默认值false
      .set("spark.streaming.backpressure.pid.minRate", "1") // 可以估算的最低费率是多少。默认值为 100，只能设置成非负值。//最小摄入条数控制
    //.set("spark.streaming.backpressure.initRate", "1") // 启用反压机制时每个接收器接收第一批数据的初始最大速率。默认值没有设置。


    val ssc = new StreamingContext(conf, Seconds(2))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = mutable.HashMap[String, String]()
    //必须添加以下参数，否则会报错
    kafkaParams.put("bootstrap.servers", brokers)
    kafkaParams.put("group.id", "directKafka_test_g1")
    kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams
      )
    )
    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}
