package com.java.spark_core.practice;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:57 上午 2020/3/2
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class LambdaStreamTestSpark {
    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("LambdaStreamTestSpark");
        JavaSparkContext sc = new JavaSparkContext(config);

        List<String> list1 = new ArrayList<>();
        list1.add("zhangsan");
        list1.add("lisi");

        List<String> list2 = new ArrayList<>();
        list2.add("lisi");

        List<String> list3 = new ArrayList<>();
        list3 = list1.stream().map(x -> {
                    if (!list2.contains(x)) {
                        return x;
                    } else {
                        return "";
                    }
                }

        ).filter(x -> x.length() > 0).collect(Collectors.toList());


        System.out.println(list3);
    }
}
