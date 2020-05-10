//package com.scala.ss_exactly_once
//
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.util.Try
//
///**
// * @Author: Lei
// * @E-mail: 843291011@qq.com
// * @Date: Created in 9:20 上午 2020/5/10
// * @Version: 1.0
// * @Modified By:
// * @Description:
// */
//object StreamApp {
//  def main(args: Array[String]): Unit = {
//
//    // 1、从kafka拿数据
//    if (args.length < 5) {
//      System.err.println("Usage: KafkaDerectStream \n" +
//        "<batch-duration-in-seconds> \n" +
//        "<kafka-bootstrap-server> \n" +
//        "<kafka-topic> \n" +
//        "<kafka-consumer-group-id> \n" +
//        "<kafka-zookeeper-quorum> "
//      )
//      System.exit(1)
//    }
//
//    // TODO
//    // val time = TimeUtils.getNowDataMin
//
//    val batchDuration: String = args(0)
//    val bootstrapServers: String = args(1).toString
//    val topicsSet: Set[String] = args(2).toString.split(",").toSet
//    val consumerGroupID: String = args(3)
//    val zkQuorum: String = args(4)
//    val sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamApp")
//    sparkconf.registerKryoClasses( // 使用kryo序列化方式
//      Array(
//        classOf[String]
//        //classOf[OrderInfo],
//        //classOf[DriverInfo],
//        //classOf[RegisterUsers]
//      )
//    )
//    val session: SparkSession = SparkSession.builder().config(sparkconf).getOrCreate()
//    val sc: SparkContext = session.sparkContext
//    val ssc = new StreamingContext(sc, Seconds(batchDuration.toLong))
//
//    val topics: Array[String] = topicsSet.toArray
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> bootstrapServers,
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> consumerGroupID,
//      "auto.offset.reset" -> "false",
//      "enable.auto.commit" -> (false) // 禁用自动提交Offset，否则可能没正常消费完就提交了，造成数据
//    )
//
//    // KafkaUtils.createDirectStream()
//    val kafkaManager = new KafkaManager(zkQuorum, kafkaParams)
//    val directStream: InputDStream[ConsumerRecord[String, String]] = kafkaManager.createDirectStream(ssc, topics)
//
//    /**
//     * maxWell 实时将MySQL中的数据，通过binlog，再解析成json，最后同步到kafka
//     * spark streaming消费kafka中的数据
//     */
//    // 处理业务逻辑
//    directStream.foreachRDD(rdd => {
//      if (!rdd.isEmpty()) {
//        val doElse = Try {
//          val data: RDD[String] = rdd.map(line => line.value())
//          // 将处理分析完的数据写入hbase
//          // 构建连接
//          data.foreachPartition(pa => {
//            println(pa)
//          })
//          // 销毁连接
//        }
//
//        if (doElse.isSuccess) {
//          kafkaManager.persistOffset(rdd)
//        }
//      }
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
