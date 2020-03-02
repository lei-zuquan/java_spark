package com.scala.spark.checkpoint

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 6:14 下午 2020/3/2
 * @Version: 1.0
 * @Modified By:
 * @Description: checkpoint
 */

/**
 * Spark中对于数据的保存除了持久化操作之外，还提供了一种检查点的机制，检查点（本质是通过将RDD写入Disk做检查点）是为了通过lineage做容错的辅助，
 * lineage过长会造成容错成本过高，这样就不如在中间阶段做检查点容错，如果之后有节点出现问题而丢失分区，从做检查点的RDD开始重做Lineage，就会减少开销。
 * 检查点通过将数据写入到HDFS文件系统实现了RDD的检查点功能。
 * 为当前RDD设置检查点。该函数将会创建一个二进制的文件，并存储到checkpoint目录中，该目录是用SparkContext.setCheckpointDir()设置的。
 * 在checkpoint的过程中，该RDD的所有依赖于父RDD中的信息将全部被移除。对RDD进行checkpoint操作并不会马上被执行，必须执行Action操作才能触发。
 */

object Spark16_Checkpoint {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("checkpoint")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    //设置检查点的保存目录; 实际项目中会采用hdfs目录，因为会存3个副本
    //  sc.setCheckpointDir("hdfs://hadoop:9000/checkpoint")
    sc.setCheckpointDir("cp")

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5))

    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    val reduceRDD = mapRDD.reduceByKey(_ + _)
    mapRDD.checkpoint()

    /**
     * 无checkpoint:
     * (4) ShuffledRDD[2] at reduceByKey at Spark16_Checkpoint.scala:37 []
     * +-(4) MapPartitionsRDD[1] at map at Spark16_Checkpoint.scala:35 []
     * |  ParallelCollectionRDD[0] at makeRDD at Spark16_Checkpoint.scala:33 []
     *
     * 有checkpoint:
     * (4) ShuffledRDD[2] at reduceByKey at Spark16_Checkpoint.scala:37 []
     * +-(4) MapPartitionsRDD[1] at map at Spark16_Checkpoint.scala:35 []
     * |  ReliableCheckpointRDD[3] at foreach at Spark16_Checkpoint.scala:46 []
     */
    reduceRDD.foreach(println)

    println(reduceRDD.toDebugString) //debug形式看血缘关系,如果从检查点抽取数据，将看不到血缘关系了。

    //释放资源
    sc.stop()
  }
}
