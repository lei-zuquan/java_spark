package com.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WordCount {
  def main(args: Array[String]): Unit = {
    //使用idea工具开发
    //local 模式

    // 创建SparkConf()对象
    //设置Spark计算框架的运行（部署）环境
    //val config : SparkConf = new SparkConf().setMaster("yarn").setAppName("WordCount")//local[*]
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount") //local[*]

    //创建spark 上下文对象
    val sc = new SparkContext(config)

    //读取文件,将文件内容一行一行的读取出来
    //路径查找位置默认从当前的部署环境中查找
    //如果需要从本地查找 file:///opt/mudle/spark/in
    //val lines : RDD[String] = sc.textFile("hdfs://ns1004/user/mart_ipd/songdongdong/spark_learning")//hdfs://ns1004/user/mart_ipd/songdongdong/spark_learning
    val lines: RDD[String] = sc.textFile("in/wordcount.txt")

    //将一行行的数据分解一个一个的单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //RDD是 把数据处理的逻辑进行了封装
    //为了统计方便，将单词进行结构的转换
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))
    //将转换结构后的数据进行分组聚合
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    //将统计的结果打印到控制台
    val result: Array[(String, Int)] = wordToSum.collect()
    println(result)
    result.foreach(println)

  }
}
