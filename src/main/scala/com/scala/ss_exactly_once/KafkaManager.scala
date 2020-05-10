package com.scala.ss_exactly_once

import kafka.utils.ZKGroupTopicDirs
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

import scala.reflect.ClassTag

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:54 上午 2020/5/10
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
class KafkaManager(zkHost: String, kafkaParams: Map[String, Object]) extends Serializable {

  //private val (zkClient, zkConnection) = ZkUtils.createZkClientAndConnection(zkHost, 1000, 1000)

  //val zkUtils = new ZkUtils(zkClient, zkConnection, false)

  def createDirectStream[K: ClassTag, V: ClassTag](ssc: StreamingContext, topics: Seq[String])
  : InputDStream[ConsumerRecord[K, V]] = {
    // 10   fromOffset 200  untilOffset 210
    val groupId: String = kafkaParams("group.id").toString
    val offset: Map[TopicPartition, Long] = readOffset(topics, groupId)
    KafkaUtils.createDirectStream(
      ssc,
      //      PerferConsistent,
      //      ConsumerStrategies.Subscribe[K, V](topics, kafkaParams, offset)
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[K, V](topics, kafkaParams, offset)
    )
  }

  /**
   * 读取偏移量
   * topic, partition, offset
   */
  def readOffset(topics: Seq[String], groupId: String): Map[TopicPartition, Long] = {
    // offset ---- zookeeper, mysql, redis, hbase

    // map 来存储offset的映射信息

    // 获取主题和分区信息（topic, Seq[partition]）

    // 迭代

    // 一个topic 下面会有多个分区
    // 封装存储了路径

    // 获取 数据

    // 指定消费位置
    //    collection.mutable.HashMap.empty[TopicPartition, Long]
    //
    //    val topicAndPartitionMap = HashMap[TopicPartition, Long] ()


    val topicAndPartitionMap = Map[TopicPartition, Long](null, null)
    for ((k, v) <- topicAndPartitionMap.toMap) {
      // 当前offset
      val currentOffset = v;
      val earliestOffset = 0;
      val latestOffset = 100;
      if (currentOffset > latestOffset || currentOffset < earliestOffset) {
        //topicAndPartitionMap.put(k, earliestOffset)
      }
    }
    topicAndPartitionMap
  }


  // 提交Offset
  def persistOffset[K, V](rdd: RDD[ConsumerRecord[K, V]], storageOffset: Boolean = true) = {
    val groupId: String = kafkaParams("group.id").toString
    // 拿到偏移量信息
    val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    // 提交偏移量
    ranges.foreach(or => {
      val zKGroupTopicDirs = new ZKGroupTopicDirs(groupId, or.topic)
      // 拼接offset的存储路径
      val offsetPath: String = zKGroupTopicDirs.consumerOffsetDir + "/" + or.partition

      val offsetVal: Long = if (storageOffset) or.untilOffset else or.fromOffset


    })
  }
}
