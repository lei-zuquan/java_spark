package com.scala.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author: Lei
  * @E-mail: 843291011@qq.com
  * @Date: 2020-03-12 10:05
  * @Version: 1.0
  * @Modified By:
  * @Description: SparkStreaming转换操作transform
  */
object SparkStreaming07_transform {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming07_transform")

    var streamingContext: StreamingContext = new StreamingContext(config, Seconds(3)) //3 秒钟，伴生对象，不需要new

    //保存数据的状态，设置检查点路径
    streamingContext.sparkContext.setCheckpointDir("cp")

    var socketLineStreaming: ReceiverInputDStream[String] = streamingContext.socketTextStream("linux1", 9999) //一行一行的接受

    //转换

    //TODO 代码（Driver)
    socketLineStreaming.map {
      case x => {
        //TODO 代码（Executer)
        x
      }
    }

    //    //TODO 代码（Driver)
    //    socketLineStreaming.transform{
    //      case rdd=>{
    //        //TODO 代码（Driver)(m  运行采集周期 次)
    //        rdd.map{
    //          case x=>{
    //            //TODO 代码 （Executer)
    //            x
    //          }
    //        }
    //      }
    //    }

    socketLineStreaming.foreachRDD(rdd => {
      rdd.foreach(println)
    })

    //打印结果
    //    stateDStream.print()

    //streamingContext.stop()  //不能停止我们的采集功能

    //启动采集器
    streamingContext.start()

    //Drvier等待采集器停止，
    streamingContext.awaitTermination()

    //nc -lc 99999   linux 下往9999端口发数据。

  }
}
