package com.java.spark_core.file_read_save;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Option;
import scala.util.parsing.json.JSON;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:31 下午 2020/3/5
 * @Version: 1.0
 * @Modified By:
 * @Description: Json文件
 */

/**
 * 如果JSON文件中每一行就是一个JSON记录，那么可以通过将JSON文件当做文本文件来读取，然后利用相关的JSON库对每一条数据进行JSON解析。
 * 注意：使用RDD读取JSON文件处理很复杂，同时SparkSQL集成了很好的处理JSON文件的方式，所以应用中多是采用SparkSQL处理JSON文件。
 * <p>
 * 导入解析json所需的包
 * import scala.util.parsing.json.JSON
 */

public class Java_Spark17_JSON {
    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("json");
        JavaSparkContext sc = new JavaSparkContext(config);

        //val json = sc.textFile("in/user.json")
        JavaRDD<String> json = sc.textFile("in/user.json");

        //val result = json.map(JSON.parseFull)
        JavaRDD<Option<Object>> result = json.map(t -> JSON.parseFull(t));

        //result.foreach(println)
        result.foreach(t -> System.out.println(t));

        //释放资源
        sc.stop();
    }
}
