package com.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:04 上午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: sample算子 作用：以指定的随机种子随机抽样出数量为fraction的数据，withReplacement表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样
 */
object Spark09_Oper8_sample {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sample")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    // map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。
    // 从指定数据集合中进行抽样处理。
    var listRDD: RDD[Int] = sc.makeRDD(1 to 10) // 这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数

    // var SampleRDD: RDD[Int] = listRDD.sample(false, 0.4, 1)//不放回
    /**
     * 1、withReplacement：元素可以多次抽样(在抽样时替换)
     * 2、fraction：期望样本的大小作为RDD大小的一部分，
     *      当withReplacement=false时：选择每个元素的概率;分数一定是[0,1] ；
     *      当withReplacement=true时：选择每个元素的期望次数; 分数必须大于等于0。
     * 3、seed：随机数生成器的种子
     *      第三个参数建议采用默认，不好把控
     */
    // var SampleRDD: RDD[Int] = listRDD.sample(true, 4, 1)// 放回抽样，可重复
    var SampleRDD = listRDD.sample(true, 0.8)

    SampleRDD.collect().foreach(println)
  }
}
