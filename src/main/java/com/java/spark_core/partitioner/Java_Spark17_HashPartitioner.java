package com.java.spark_core.partitioner;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:19 下午 2020/3/2
 * @Version: 1.0
 * @Modified By:
 * @Description: 使用Hash分区的实操
 */

/**
 * HashPartitioner分区的原理：对于给定的key，计算其hashCode，并除以分区的个数取余，
 * 如果余数小于0，则用余数+分区的个数（否则加0），最后返回的值就是这个key所属的分区ID。
 * <p>
 * HashPartitioner分区弊端：可能导致每个分区中数据量的不均匀，极端情况下会导致某些分区拥有RDD的全部数据。
 */
public class Java_Spark17_HashPartitioner {
    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("Spark17_HashPartitioner");
        JavaSparkContext sc = new JavaSparkContext(config);

        //val nopar = sc.parallelize(List((1, 3), (1, 2), (2, 4),(2, 3),(3, 6),(3, 8)),8)
        List<Tuple2<Integer, Integer>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(1, 3));
        list1.add(new Tuple2<>(1, 2));
        list1.add(new Tuple2<>(2, 4));
        list1.add(new Tuple2<>(2, 3));
        list1.add(new Tuple2<>(3, 6));
        list1.add(new Tuple2<>(3, 8));
        JavaPairRDD<Integer, Integer> nopar = sc.parallelizePairs(list1, 8);


        // nopar.mapPartitionsWithIndex((index, iter) =>{
        //     Iterator(index.toString + " : " + iter.mkString("|"))
        // }).collect.foreach(println)
        nopar.mapPartitionsWithIndex((v1, v2) -> {
            HashMap<Integer, ArrayList<Tuple2<Integer, Integer>>> map = new HashMap<Integer, ArrayList<Tuple2<Integer, Integer>>>();
            ArrayList<Tuple2<Integer, Integer>> list = null;
            while (v2.hasNext()) {
                Tuple2<Integer, Integer> next = v2.next();
                if (map.containsKey(v1) && list != null) {
                    ArrayList<Tuple2<Integer, Integer>> tmpList = map.get(v1);
                    tmpList.add(next);
                    map.put(v1, tmpList);
                } else {
                    list = new ArrayList<Tuple2<Integer, Integer>>();
                    list.add(next);
                    map.put(v1, list);
                }
            }

            Iterator<Integer> iterator = map.keySet().iterator();
            HashSet<Tuple2<Integer, ArrayList<Tuple2<Integer, Integer>>>> set = new HashSet<Tuple2<Integer, ArrayList<Tuple2<Integer, Integer>>>>();
            while (iterator.hasNext()) {
                int next = iterator.next();
                set.add(new Tuple2<Integer, ArrayList<Tuple2<Integer, Integer>>>(next, map.get(next)));
            }
            return set.iterator();
        }, false).collect().forEach(System.out::println);

        System.out.println("----------------------------------------");
        //val hashpar = nopar.partitionBy(new org.apache.spark.HashPartitioner(7))
        JavaPairRDD<Integer, Integer> hashpar = nopar.partitionBy(new HashPartitioner(7));

        System.out.println("hashpar.count:" + hashpar.count());

        System.out.println("hashpar.partitioner.toString:" + hashpar.partitioner().toString());

        //hashpar.mapPartitions(iter = > Iterator(iter.length)).collect().foreach(println)
        hashpar.mapPartitions(iter -> {
            int length = 0;
            while (iter.hasNext()) {
                Tuple2<Integer, Integer> next = iter.next();
                System.out.println("tuple._1: " + next._1 + " tuple._2: " + next._2);
                length++;
            }
            List<Integer> list = new ArrayList<>();
            list.add(length);
            return list.iterator();
        }).collect().forEach(System.out::println);
    }
}
