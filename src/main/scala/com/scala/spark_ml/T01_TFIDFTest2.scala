package com.scala.spark_ml

import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:37 上午 2020/7/6
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// 从文件中加载需要分析的数据


case class Record(
                   id: String,
                   companyName: String,
                   direction: String,
                   productInfo: String
                 )

object T01_TFIDFTest2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("T01_TFIDFTest1")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //import sqlContext.implicits._
    import sqlContext.implicits._

    val records: DataFrame = sc.textFile("in/spark_ml_file")
      .map { x =>
        val data: Array[String] = x.split(",")
        Record(data(0), data(1), data(2), data(3))
      }.toDF().cache()


    val wordData = new Tokenizer().setInputCol("productInfo").setOutputCol("productWords").transform(records)
    //wordData.show()

    // setNumFeatures(20) // 向量的维数，聚类的数量，特征值
    // TF: HashingTF是一个transfomer，在文本处理中，接受词条的集合，然后把这样集合转化为固定长度的特征向量。
    val hashingTF = new HashingTF().setInputCol("productWords").setOutputCol("productFeatures").setNumFeatures(20)
    // 特征数据
    val tfData: DataFrame = hashingTF.transform(wordData)
    tfData.show()

    val idf: IDF = new IDF().setInputCol("productFeatures").setOutputCol("features")
    // 一般模型计算完毕我们会保存到hdfs中，为了以后数据的加载模型计算。

    // TF: Estimator，在一个数据集上应用的fit() 方法，产生一个IDFModel，用该模型接收特征向量，然后计算每一个词在文档中出现的频次。
    val idfModel: IDFModel = idf.fit(tfData)
    idfModel.save("in/spark_ml_file_tfidfteat2")


    val rescaledData: DataFrame = idfModel.transform(tfData)
    rescaledData.select("id", "companyName", "features").show()

    /*
    [(20,[0,8,9,18],[0.6931471805599453,0.6931471805599453,0.28768207245178085,0.28768207245178085]),0]
    20是向量数，在0位置，TF-IDF值为0.6931471805599453；然后除了0、8、9、18外其他TF-IDF都是0
     */

  }
}
