package com.scala.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
  * @Author: Lei
  * @E-mail: 843291011@qq.com
  * @Date: 2020-03-12 9:29
  * @Version: 1.0
  * @Modified By:
  * @Description:
  */
object SparkStreaming02_FileDataSource {
  def main(args: Array[String]): Unit = {
    // 使用SparkStreaming
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming02_FileDataSource")
    var streamingContext: StreamingContext = new StreamingContext(config, Seconds(3))//3 秒钟，伴生对象，不需要new

    // 从文件夹中采集数据
    var fileDStreaming: DStream[String] = streamingContext.textFileStream("test")

    // 将采集的数据进行分解（偏平化）
    var wordDstream: DStream[String] = fileDStreaming.flatMap(line => line.split(" "))//偏平化后，按照空格分割

    // 将我们的数据进行转换方便分析
    var mapDstream: DStream[(String, Int)] = wordDstream.map((_, 1))

    // 将转换后的数据聚合在一起处理
    var wordToSumStream: DStream[(String, Int)] = mapDstream.reduceByKey(_ + _)

    // 打印结果
    wordToSumStream.print()

    // 启动采集器
    streamingContext.start()
    // Driver 等待采集器停止，
    streamingContext.awaitTermination()
  }
}
