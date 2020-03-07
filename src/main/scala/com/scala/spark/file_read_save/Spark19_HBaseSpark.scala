package com.scala.spark.file_read_save

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 12:04 上午 2020/3/8
 * @Version: 1.0
 * @Modified By:
 * @Description: Spark core 操作Hbase 示例
 */

/*
由于 org.apache.hadoop.hbase.mapreduce.TableInputFormat 类的实现，Spark 可以通过Hadoop输入格式访问HBase。
这个输入格式会返回键值对数据，其中键的类型为org. apache.hadoop.hbase.io.ImmutableBytesWritable，
而值的类型为org.apache.hadoop.hbase.client.Result。
 */
object Spark19_HBaseSpark {

  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkHbase")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)
    //构建HBase配置信息
    val conf: Configuration = HBaseConfiguration.create()

    val zk_list = "node-03,node-01,node-02"
    conf.set("hbase.zookeeper.quorum", zk_list)
    // 设置连接参数:hbase数据库使用的接口
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    /*
    hbase shell:进入hbase客户端创建表

    create 'spark_learn_student','info'
    put 'spark_learn_student','1001','info:name','zhangsan'
     */
    conf.set(TableInputFormat.INPUT_TABLE, "spark_learn_student")

    //从HBase读取数据形成RDD
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val count: Long = hbaseRDD.count()
    println(count)

    //对hbaseRDD进行处理
    hbaseRDD.foreach {
      case (_, result) =>
        val key: String = Bytes.toString(result.getRow)
        val name: String = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
        val color: String = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("color")))
        println("RowKey:" + key + ",Name:" + name + ",Color:" + color)
    }

    //关闭连接
    sc.stop()

  }

}
