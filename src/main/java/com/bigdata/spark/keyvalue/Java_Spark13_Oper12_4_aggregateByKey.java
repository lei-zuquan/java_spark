package com.bigdata.spark.keyvalue;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 5:01 下午 2020/3/1
 * @Version: 1.0
 * @Modified By:
 * @Description: aggregateByKey算子 将每个分区里面的元素进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。这个函数最终返回的类型不需要和RDD中元素类型一致。
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 参数：(zeroValue:U,[partitioner: Partitioner]) (seqOp: (U, V) => U,combOp: (U, U) => U)
 * 1. 作用：在kv对的RDD中，，按key将value进行分组合并，合并时，将每个value和初始值作为seq函数的参数，进行计算，
 * 返回的结果作为一个新的kv对，然后再将结果按照key进行合并，最后将每个分组的value传递给combine函数进行计算
 * （先将前两个value进行计算，将返回结果和下一个value传给combine函数，以此类推），将key与计算结果作为一个新的kv对输出。
 */
public class Java_Spark13_Oper12_4_aggregateByKey {
    public static void main(String[] args) {
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("aggregateByKey");
        JavaSparkContext sc = new JavaSparkContext(config);

        //val rdd = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>("a", 3));
        list.add(new Tuple2<>("a", 2));
        list.add(new Tuple2<>("c", 4));
        list.add(new Tuple2<>("c", 6));
        list.add(new Tuple2<>("c", 8));
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(list);

        // glom算子，将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
        //rdd.glom().foreach(array => {
        //        println(array.mkString(","))
        //})
        System.out.println("rdd有：" + rdd.getNumPartitions() + " 分区");
        rdd.glom().collect().forEach(glomList -> {
            System.out.println(glomList);
            System.out.println("********************");
        });

        //println("=========================")
        System.out.println("=========================");
        /**
         * 创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加
         * （1）zeroValue：给每一个分区中的每一个key一个初始值；
         * （2）seqOp：函数用于在每一个分区中用初始值逐步迭代value；
         * （3）combOp：函数用于合并每个分区中的结果。
         */
        //val agg = rdd.aggregateByKey(0)(math.max(_, _), _ + _)
        /**
         rdd.aggregateByKey(14, new Function2<Integer, Integer, Integer>() {
         public Integer call(Integer v1, Integer v2) throws Exception {
         System.out.println("seqOp>>>>>  参数One："+v1+"--参数Two:"+v2);
         return Math.max(v1,v2);
         }
         }, new Function2<Integer, Integer, Integer>() {
         public Integer call(Integer v1, Integer v2) throws Exception {
         System.out.println("combOp>>>>>  参数One："+v1+"--参数Two:"+v2);
         return v1+v2;
         }
         });
         */

        JavaPairRDD<String, Integer> agg = rdd.aggregateByKey(0,
                (v1, v2) -> Math.max(v1, v2),
                (v1, v2) -> v1 + v2
        );


        // agg.collect().foreach(println)
        agg.collect().forEach(System.out::println);
    }
}
