package com.bigdata.spark.keyvalue

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 3:54 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: reduceByKey算子 作用：在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起，reduce任务的个数可以通过第二个可选的参数来设置
 */

/**
 * reduceByKey和groupByKey的区别
 *      1. reduceByKey：按照key进行聚合，在shuffle之前有combine（本地预聚合）操作，返回结果是RDD[k,v].
 *      2. groupByKey：按照key进行分组，直接进行shuffle。
 *      3. 开发指导：reduceByKey比groupByKey，建议使用。但是需要注意是否会影响业务逻辑。
 */
object Spark13_Oper12_reduceByKey {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("reduceByKey");

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    val rdd = sc.parallelize(List(("female", 1), ("male", 5), ("female", 5), ("male", 2)))
    val reduce = rdd.reduceByKey((x, y) => x + y)

    reduce.collect().foreach(println)

  }
}
