package com.scala.spark_streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: Lei
  * @E-mail: 843291011@qq.com
  * @Date: 2020-03-12 9:46
  * @Version: 1.0
  * @Modified By:
  * @Description:
  */
object SparkStreaming03_MyReceiver {
  def main(args: Array[String]): Unit = {
    //使用SparkStreaming
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming03_MyReceiver")
    var streamingContext: StreamingContext = new StreamingContext(config, Seconds(3)) //3 秒钟，伴生对象，不需要new

    //从自定义采集器中采集数据
    var FileDStreaming: DStream[String] = streamingContext.receiverStream(new MyReceiver("linux1", 99999))


    //将采集的数据进行分解（偏平化）
    var WordDstream: DStream[String] = FileDStreaming.flatMap(line => line.split(" ")) //偏平化后，按照空格分割

    //将我们的数据进行转换方便分析
    var mapDstream: DStream[(String, Int)] = WordDstream.map((_, 1))

    //    //将转换后的数据聚合在一起处理
    //    var stateDStream: DStream[(String, Int)] = mapDstream.updateStateByKey {
    //      case (seq, buffer) => {
    //        var sum = buffer.getOrElse(0) + seq.sum
    //        Option(sum)
    //      }
    //    }

    // 将转换后的数据聚合在一起处理
    var wordToSumStream: DStream[(String, Int)] = mapDstream.reduceByKey(_ + _)

    // 打印结果
    wordToSumStream.print()

    // 启动采集器
    streamingContext.start()
    // Driver等待采集器停止，
    streamingContext.awaitTermination()
  }

}

// 声明采集器
// 1.继承Receiver
class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  //scala 构造方法

  var socket: Socket = null

  def receiver(): Unit = {
    socket = new Socket(host, port)

    var reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))

    var line: String = null

    while ((line == reader.readLine()) != null) {
      //将采集的数据，存储到采集器的内部
      if ("END".equals(line)) {
        return
      } else {
        this.store(line)
      }
    }

  }

  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        receiver()
      }
    }).start()

  }

  override def onStop(): Unit = {
    if (socket != null) {}
    socket.close()
    socket = null
  }
}

