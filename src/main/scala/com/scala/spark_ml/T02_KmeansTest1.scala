package com.scala.spark_ml

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 2:58 下午 2020/7/5
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
  在演示连接kafka、hdfs、tfidf、kmeans看起来代码都很简单。
  机器学习或者spark代码编写就相对简单（基于scala的函数式编程）。对于机器学习等重要的是性能调优，以及一些参数的设置。
  代码看着简单易懂也和我们的业务有关系。
 */

//case class Record(
//                   id: String,
//                   companyName: String,
//                   direction: String,
//                   productInfo: String
//                 )

object KmeansTest1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf();
    conf.setAppName("kmeans")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //import sqlContext.implicits._

    val records: DataFrame = sc.textFile("in/spark_ml_file")
      .map { x =>
        val data: Array[String] = x.split(",")
        Record(data(0), data(1), data(2), data(3))
      }.toDF().cache()

    // 数据字段提取及字段分词
    // 将productInfo的数据 去掉空格分开存储
    val wordsData: DataFrame = new Tokenizer()
      .setInputCol("productInfo")
      .setOutputCol("productWords")
      .transform(records)

    // 计算HashingTF
    val tfData: DataFrame = new HashingTF()
      .setNumFeatures(5)
      .setInputCol("productWords")
      .setOutputCol("productFeatures")
      .transform(wordsData)

    // idfModel计算
    val idfModel: IDFModel = new IDF()
      .setInputCol("productFeatures")
      .setOutputCol("features")
      .fit(tfData)

    // idfModel保存
    val idfData: DataFrame = idfModel.transform(tfData)
    val trainingData: DataFrame = idfData.select("id", "companyName", "features")

    val kMeans: KMeans = new KMeans()
      .setK(3)
      .setMaxIter(50)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    val kMeansModel: KMeansModel = kMeans.fit(trainingData)
    // kMeansModel.save("")

    val kMeansData: DataFrame = kMeansModel.transform(trainingData).cache()
    kMeansData.show()

  }

}















