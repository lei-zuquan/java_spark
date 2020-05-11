package com.java.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducerTest {

	// kafka-topics --create --zookeeper node-01:2181,node-02:2181,node-03:2181 --replication-factor 3 --partitions 3 --topic myTopicTest
	// kafka-topics --zookeeper node-01:2181,node-02:2181,node-03:2181 --topic myTopicTest --describe
	// kafka-console-producer --broker-list node-01:9092,node-02:9092,node-03:9092 --topic myTopicTest

	// 默认情况下，从版本0.11.0.0开始，Kafka选择第一个策略并支持等待一致的副本。
	// 可以使用配置属性unclean.leader.election.enable更改此行为
	// 1.等待ISR中的副本恢复生机并选择此副本作为领导者（希望它仍然拥有其所有数据）。
	// 2.选择作为领导者的第一个复制品（不一定在ISR中）。

	// 上述配置，这有效地优先于消息丢失风险的不可用性
	public static void main(String[] args) throws Exception {

		Properties props = new Properties();
		props.put("bootstrap.servers", "node-01:9092,node-02:9092,node-03:9092");
		props.put("acks", "all"); //默认情况下，当acks = all时，只要所有当前的同步内副本都收到消息，就会发生确认。
		props.put("delivery.timeout.ms", 30000);
		props.put("request.timeout.ms", 20000);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		/*
		 * 可选配置，如果不配置，则使用默认的partitioner partitioner.class
		 * 默认值：kafka.producer.DefaultPartitioner
		 * 用来把消息分到各个partition中，默认行为是对key进行hash。
		 */
		props.put("partitioner.class", "com.java.kafka.DefineMyKafkaPartitioner");
		// props.put("partitioner.class", "kafka.producer.DefaultPartitioner");

		Producer<String, String> producer = new KafkaProducer<>(props);
		String topic = "directKafka_test";

		for (int i = 0; i < 600000; i++) {
			//producer.send(new ProducerRecord<String, String>(
			//		topic, 
			//i, // 如果key没有填写的话，默认是null；按照key进行哈希，相同key去一个partition。（如果扩展了partition的数量那么就不能保证了）
			//		"hello world"));
			//for (int j = 0; j < 10; j++) {
			String key = i + "";
			producer.send(new ProducerRecord<>(topic, key, "helloworld"));
			TimeUnit.MILLISECONDS.sleep(1);
			//}
		}


		System.out.println(System.currentTimeMillis() + "\t\t已成功发送数据到Kafka中");

		producer.close();
	}

}
