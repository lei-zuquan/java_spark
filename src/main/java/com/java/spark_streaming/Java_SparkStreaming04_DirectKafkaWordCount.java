package com.java.spark_streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * @author Administrator
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


public class Java_SparkStreaming04_DirectKafkaWordCount {

	public static void main(String[] args) {
		/*
		  node-01:9092,node-02:9092,node-03:9092 directKafka_test

		 */
		if (args.length < 2) {
			System.err.println(
					"|Usage: DirectKafkaWordCount <brokers> <topics> "
							+ "|  <brokers> is a list of one or more Kafka brokers"
							+ "|  <topics> is a list of one or more kafka topics to consume from"
							+ "|");
			System.exit(1);
		}

		// 构建Spark Streaming上下文
		SparkConf conf = new SparkConf()
				//.setMaster("local[2]")
				.setAppName("Java_DirectKafka")
				// 设置程序优雅的关闭
				.set("spark.streaming.stopGracefullyOnShutdown", "true")
				// 设置spark streaming 从kafka消费数据的最大速率 （每秒 / 每个分区；秒  * 分区  * 值）
				// 生产环境下，测试你集群的计算极限
				.set("spark.streaming.kafka.maxRatePerPartition", "50")
				// 推荐的序列化方式
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 指定序列化方式
				.set("spark.default.parallelism", "15")         // 调节并行度
				.set("spark.streaming.blockInterval", "200")    // Spark Streaming接收器接收的数据在存储到Spark之前被分块为数据块的时间间隔; 增加block数量，增加每个batch rdd的partition数量，增加处理并行度
				.set("spark.shuffle.consolidateFiles", "true")  // 开启shuffle map端输出文件合并的机制，默认false
				.set("spark.shuffle.file.buffer", "128K")       // 调节map端内存缓冲，默认32K
				.set("spark.shuffle.memoryFraction", "0.3")      // reduce端内存占比，默认0.2
				.set("spark.scheduler.mode", "FAIR")           // 设置调度策略，默认FIFO
				.set("spark.locality.wait", "2")              // 调节本地化等待时长，默认3秒

				// .set("spark.streaming.receiver.maxRate", "1") // receiver方式接收; 限制每个 receiver 每秒最大可以接收的记录的数据
				.set("spark.streaming.kafka.maxRatePerPartition", "2000") // 限流，对目标topic每个partition每秒钟拉取的数据条数 条数/（每个partition）
				.set("spark.streaming.backpressure.enabled", "true") // 启用backpressure机制，默认值false
				.set("spark.streaming.backpressure.pid.minRate", "1"); // 可以估算的最低费率是多少。默认值为 100，只能设置成非负值。//最小摄入条数控制
		//.set("spark.streaming.backpressure.initRate", "1") // 启用反压机制时每个接收器接收第一批数据的初始最大速率。默认值没有设置。
		//.set("spark.streaming.receiver.writeAheadLog.enable", "true");   


		// spark streaming的上下文是构建JavaStreamingContext对象
		JavaStreamingContext jssc = new JavaStreamingContext(
				conf, Durations.seconds(2));

		// 创建SparkSession
		SparkSession sparkSession = new SparkSession(jssc.ssc().sc());


		// 创建针对Kafka数据来源的输入DStream（离线流，代表了一个源源不断的数据来源，抽象）
		// 选用kafka direct api（很多好处，包括自己内部自适应调整每次接收数据量的特性，等等）
		String brokers = args[0];
		String topic = args[1];

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", brokers);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", topic + "_g1"); //_g1Test 本地测试
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", true);

		Collection<String> topics = Arrays.asList(topic);

		// 从kafka中的ConsumerRecord获取数据
		// 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
		// 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
		JavaInputDStream<ConsumerRecord<String, String>> messages =
				KafkaUtils.createDirectStream(
						jssc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
				);

		// Get the lines, split them into words, count the words and print

		JavaDStream<String> wordDStream = messages.map(record -> record.value()).flatMap(line -> {
					String[] words = line.split(" ");
					TimeUnit.MILLISECONDS.sleep(2);
					return Arrays.asList(words).iterator();
				}
		);

		wordDStream.map(word -> new Tuple2<String, Integer>(word, 1)).
				reduce((t1, t2) -> {

							return new Tuple2<String, Integer>(t1._1, t1._2 + t2._2);
						}
				).print();

		// Start the computation

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (Exception e) {
			e.printStackTrace();
		}

		sparkSession.close();
		jssc.close();
	}


}
