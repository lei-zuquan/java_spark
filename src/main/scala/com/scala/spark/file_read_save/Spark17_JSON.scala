package com.scala.spark.file_read_save

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:30 下午 2020/3/5
 * @Version: 1.0
 * @Modified By:
 * @Description: Json文件
 */

/**
 * 如果JSON文件中每一行就是一个JSON记录，那么可以通过将JSON文件当做文本文件来读取，然后利用相关的JSON库对每一条数据进行JSON解析。
 * 注意：使用RDD读取JSON文件处理很复杂，同时SparkSQL集成了很好的处理JSON文件的方式，所以应用中多是采用SparkSQL处理JSON文件。
 *
 * 导入解析json所需的包
 * import scala.util.parsing.json.JSON
 */
object Spark17_JSON {

  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("json")

    // 创建Spark上下文对象
    var sc: SparkContext = new SparkContext(config)

    val json = sc.textFile("in/user.json")

    val result = json.map(JSON.parseFull)

    result.foreach(println)

    //释放资源
    sc.stop()

  }
}
