package com.java.spark_core.partitioner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:09 下午 2020/3/5
 * @Version: 1.0
 * @Modified By:
 * @Description: CustomerPartitioner 自定义分区器
 */

/**
 * 要实现自定义的分区器，你需要继承 org.apache.spark.Partitioner 类并实现下面三个方法。
 * （1）numPartitions: Int:返回创建出来的分区数。
 * （2）getPartition(key: Any): Int:返回给定键的分区编号(0到numPartitions-1)。
 * （3）equals():Java 判断相等性的标准方法。这个方法的实现非常重要，
 * Spark 需要用这个方法来检查你的分区器对象是否和其他分区器实例相同，这样 Spark 才可以判断两个 RDD 的分区方式是否相同。
 * 需求：将相同后缀的数据写入相同的文件，通过将相同后缀的数据分区到相同的分区并保存输出来实现。
 * <p>
 * 使用自定义的 Partitioner 是很容易的:只要把它传给 partitionBy() 方法即可。
 * Spark 中有许多依赖于数据混洗的方法，比如 join() 和 groupByKey()，它们也可以接收一个可选的 Partitioner 对象来控制输出数据的分区方式。
 */
public class Java_Spark18_CustomerPartitioner {
    public static void main(String[] args) {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("Spark18_CustomerPartitioner");
        JavaSparkContext sc = new JavaSparkContext(config);

        //（1）创建一个pairRDD
        //val data = sc.parallelize(Array((1, 1), (2, 2), (3, 3),(4, 4),(5, 5),(6, 6)))
        List<Tuple2<Integer, Integer>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(1, 1));
        list1.add(new Tuple2<>(2, 2));
        list1.add(new Tuple2<>(3, 3));
        list1.add(new Tuple2<>(4, 4));
        list1.add(new Tuple2<>(5, 5));
        list1.add(new Tuple2<>(6, 6));
        JavaPairRDD<Integer, Integer> data = sc.parallelizePairs(list1);

        //（3）将RDD使用自定义的分区类进行重新分区
        JavaPairRDD<Integer, Integer> par = data.partitionBy(new CustomerPartitioner(2));

        //（4）查看重新分区后的数据分布
        // par.mapPartitionsWithIndex((index, items) =>items.map((index, _))).collect.foreach(println)
        par.mapPartitionsWithIndex((v1, v2) -> {
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

    }
}


// （2）定义一个自定义分区类
class CustomerPartitioner extends org.apache.spark.Partitioner {

    private int numParts = 0;

    public CustomerPartitioner(int numPart) {
        this.numParts = numPart;
    }

    //覆盖分区数
    @Override
    public int numPartitions() {
        return this.numParts;
    }

    //覆盖分区号获取函数
    @Override
    public int getPartition(Object key) {
        String ckey = key.toString();
        return Integer.valueOf(ckey.substring(ckey.length() - 1)) % numParts;
    }

}