package com.java.spark_core.accumulator_broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:38 上午 2020/3/8
 * @Version: 1.0
 * @Modified By:
 * @Description: 广播变量（调优策略）
 */

/*

广播变量用来高效分发较大的对象。向所有工作节点发送一个较大的只读值，以供一个或多个Spark操作使用。
比如，如果你的应用需要向所有节点发送一个较大的只读查询表，甚至是机器学习算法中的一个很大的特征向量，
广播变量用起来都很顺手。 在多个并行操作中使用同一个变量，但是 Spark会为每个任务分别发送。

使用广播变量的过程如下：
(1) 通过对一个类型 T 的对象调用 SparkContext.broadcast 创建出一个 Broadcast[T] 对象。 任何可序列化的类型都可以这么实现。
(2) 通过 value 属性访问该对象的值(在 Java 中为 value() 方法)。
(3) 变量只会被发到各个节点一次，应作为只读值处理(修改这个值不会影响到别的节点)。

 */

public class Java_Spark22_Broadcast {
    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("Spark22_Broadcast");
        JavaSparkContext sc = new JavaSparkContext(config);

        //var rdd1: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        list.add(new Tuple2<>(1, "a"));
        list.add(new Tuple2<>(2, "b"));
        list.add(new Tuple2<>(3, "c"));
        JavaPairRDD<Integer, String> rdd1 = sc.parallelizePairs(list);

        //var rdd2: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (2, 2), (3, 3)))
        List<Tuple2<Integer, Integer>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>(1, 1));
        list2.add(new Tuple2<>(2, 2));
        list2.add(new Tuple2<>(3, 3));
        JavaPairRDD<Integer, Integer> rdd2 = sc.parallelizePairs(list2);


        // 效率很差，设计到shuffle的过程，笛卡尔乘积，效率太慢
        //JavaPairRDD<Integer, Tuple2<String, Integer>> joinRDD = rdd1.join(rdd2);
        //joinRDD.foreach(t -> System.out.println(t));

        //可以使用广播变量减少数据的传输
        //构建广播变量
        //共享的只读变量，减少数据传输的总量
        Broadcast<List<Tuple2<Integer, Integer>>> broadcast = sc.broadcast(list2);

        //map没有shuffle
        JavaRDD<Tuple2<Integer, Tuple2<String, Integer>>> resultRDD = rdd1.map(tuple -> {
            Integer key = tuple._1;
            String value = tuple._2;
            for (Tuple2<Integer, Integer> bc : broadcast.value()) {
                if (tuple._1.intValue() == bc._1.intValue()) {
                    return new Tuple2<Integer, Tuple2<String, Integer>>(key, new Tuple2<String, Integer>(value, bc._2));
                }
            }

            return new Tuple2<Integer, Tuple2<String, Integer>>(key, new Tuple2<String, Integer>(value, -1));
        });

        resultRDD.foreach(t -> System.out.println(t));
        //释放资源
        sc.stop();
    }
}
