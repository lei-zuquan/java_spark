package com.java.spark_core.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:43 上午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: groupBy算子 作用：分组，按照传入函数的返回值进行分组。将相同的key对应的值放入一个迭代器。
 */
public class Java_Spark07_Oper6_groupByV2 {
    public static void main(String[] args) {
        // var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupBy")
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("groupBy");

        // 创建Spark上下文对象
        // var sc: SparkContext = new SparkContext(config)
        JavaSparkContext sc = new JavaSparkContext(config);

        // map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。
        // 生成数据，并按指定规则进行分组
        // 分组后的数据，形成对偶元组（k-v),k表示分组的key  , value 表示分组的集合
        // var listRDD: RDD[Int] = sc.makeRDD(1 to 16) // 这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数
        List<Integer> list = new ArrayList();
        for (int i = 1; i <= 16; i++) {
            list.add(i);
        }
        JavaRDD<Integer> listRDD = sc.parallelize(list, 4);

        List<Tuple2<String, String>> listV = Arrays.asList(
                new Tuple2<String, String>("a", "a 1value"),
                new Tuple2<String, String>("a", "a 2value"),
                new Tuple2<String, String>("a", "a 3value"),
                new Tuple2<String, String>("b", "b 1value"),
                new Tuple2<String, String>("b", "b 4value"),
                new Tuple2<String, String>("a", "a 5value"),
                new Tuple2<String, String>("b", "b 2value"),
                new Tuple2<String, String>("c", "c 1value"),
                new Tuple2<String, String>("a", "a 6value")
        );

        JavaRDD<Tuple2<String, String>> parallelize = sc.parallelize(listV, 4);
        JavaPairRDD<String, String> pairRDD = parallelize.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringIntegerTuple2) throws Exception {
                return new Tuple2<String, String>(stringIntegerTuple2._1, stringIntegerTuple2._2);
            }
        });

        JavaPairRDD<String, Iterable<String>> grouped = pairRDD.groupByKey();

        JavaRDD<Tuple2<String, Iterable<String>>> res = grouped.mapPartitionsWithIndex(
                (Integer v1, Iterator<Tuple2<String, Iterable<String>>> v2) -> {
                    List<Tuple2<String, Iterable<String>>> returnList = new ArrayList<>();
                    while (v2.hasNext()) {
                        Tuple2<String, Iterable<String>> next = v2.next();
                        String key = next._1;
                        returnList.add(new Tuple2<String, Iterable<String>>("分区：" + v1 + " " + key, next._2));
                    }
                    return returnList.iterator();
                }, true);


        Iterator<Tuple2<String, Iterable<String>>> iterator = res.collect().iterator();
        while (iterator.hasNext()) {
            Tuple2<String, Iterable<String>> next = iterator.next();
            Iterable<String> integerIterator = next._2;

            String str = "\t";
            Iterator<String> iterator1 = integerIterator.iterator();
            while (iterator1.hasNext()) {
                String value = iterator1.next();
                str = str + value + "\t";
            }
            System.out.println(next._1 + str);
        }

        sc.close();
    }
}
