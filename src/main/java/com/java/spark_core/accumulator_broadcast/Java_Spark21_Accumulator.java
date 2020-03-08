package com.java.spark_core.accumulator_broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:11 上午 2020/3/8
 * @Version: 1.0
 * @Modified By:
 * @Description: 自定义累加器
 */

/*
自定义累加器类型的功能在1.X版本中就已经提供了，但是使用起来比较麻烦，在2.0版本后，累加器的易用性有了较大的改进，
而且官方还提供了一个新的抽象类：AccumulatorV2来提供更加友好的自定义类型累加器的实现方式。实现自定义类型累加器
需要继承AccumulatorV2并至少覆写下例中出现的方法，下面这个累加器可以用于在程序运行过程中收集一些文本类信息，最
终以Set[String]的形式返回。
 */

public class Java_Spark21_Accumulator {
    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("Spark21_Accumulator");
        JavaSparkContext sc = new JavaSparkContext(config);

        //val dataRDD: RDD[String] = sc.makeRDD(List("Hadoop", "hive", "Habsde", "scala", "hello"), 2)
        JavaRDD<String> dataRDD = sc.parallelize(Arrays.asList("Hadoop", "hive", "Habsde", "scala", "hello"), 2);

        // TODO 创建累加器
        WordAccumulator wordAccumulator = new WordAccumulator();
        // TODO 注册累加器
        // sc.register(wordAccumulator);
        sc.sc().register(wordAccumulator);

        dataRDD.foreach(word -> {
            // TODO 执行累加器的累加功能
            wordAccumulator.add(word);
        });

        // TODO 获取累加器的值
        System.out.println("sum =  " + wordAccumulator.value());
        //释放资源
        sc.close();
    }
}

// 声明累加器
// 1. 继承一个累加器抽象类AccumulatorV2
// 2. 实现抽象方法
// 声明(创建）累加器
class WordAccumulator extends AccumulatorV2<String, List<String>> {

    List<String> list = new ArrayList<>();

    // 当前累加器是否是初始化状态
    @Override
    public boolean isZero() {
        return list.isEmpty();
    }

    // 复制累加器对象
    @Override
    public AccumulatorV2<String, List<String>> copy() {
        return new WordAccumulator();
    }

    // 重置累加器对象
    @Override
    public void reset() {
        list.clear();
    }

    // 向累加器中增加数据
    @Override
    public void add(String v) {
        if (v.contains("h")) {
            list.add(v);
        }
    }

    // 合并累加器
    @Override
    public void merge(AccumulatorV2<String, List<String>> other) {
        list.addAll(other.value());
    }

    // 获取累加器的结果
    @Override
    public List<String> value() {
        return list;
    }
}
