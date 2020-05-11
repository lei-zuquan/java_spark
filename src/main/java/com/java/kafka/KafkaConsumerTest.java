package com.java.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerTest {


	@SuppressWarnings({"resource"})
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "node-01:9092,node-02:9092,node-03:9092");
		props.put("group.id", "directKafka_test");
		// 1.earliest：自动将偏移重置为最早的偏移量
		// 2.latest：自动将偏移量重置为最新的偏移量（默认）
		props.put("auto.offset.reset", "latest");
		props.put("enable.auto.commit", true); // true的话,Kafka会自动提交消费
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		// 可以订阅多个topic数据
		String topic = "directKafka_test";
		consumer.subscribe(Arrays.asList(topic));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, Partition = %d, key = %s, value = %s%n",
						record.offset(),
						record.partition(),
						record.key(),
						//record.value());
						"");
			}
		}
	}

}
