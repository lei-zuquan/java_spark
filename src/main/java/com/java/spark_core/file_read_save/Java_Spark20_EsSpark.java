package com.java.spark_core.file_read_save;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-13 10:29
 * @Version: 1.0
 * @Modified By:
 * @Description: Spark core 操作Hbase 示例
 */

class StudentInfo implements Serializable {
    public String title;

    public StudentInfo() {
    }

    public StudentInfo(String title) {
        this.title = title;
    }

    public StudentInfo of(String title) {
        return new StudentInfo(title);
    }
}

public class Java_Spark20_EsSpark implements Serializable {
    public static void main(String[] args) throws Exception {
        // 1.初始化spark配置信息并建立与spark的连接
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Java_Spark20_EsSpark");
        sparkConf.set("es.index.auto.create", "true");
        sparkConf.set("es.internal.es.version", "6.2.4");
        sparkConf.set("es.nodes.discovery", "false");
        sparkConf.set("es.nodes.wan.only", "false");
        sparkConf.set("es.nodes", "172.19.126.252,172.19.126.254,172.19.125.200");
        //---->如果是连接的远程es节点，该项必须要设置  
        sparkConf.set("es.port", "9300");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        //SparkSession session = new SparkSession(sc.sc()); // 方法私有，不能正常创建
        SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();

        List<StudentInfo> list = new ArrayList<>();
        list.add(new StudentInfo("zhangsan"));
        list.add(new StudentInfo("lisi"));
        list.add(new StudentInfo("wangwu"));
        JavaRDD<StudentInfo> userRDD = sc.parallelize(list, 2);

        JavaEsSpark.saveToEs(userRDD, "test1/word");

//        Encoder<StudentInfo> userEncoder = Encoders.bean(StudentInfo.class);
//        Dataset<StudentInfo> studentDS = session.createDataset(list, userEncoder);
//
//        Map<String, Object> params = new HashMap<>();
//        // 指定id
//        //params.put("es.mapping.id", "1");
//        // 主机地址
//        params.put("es.nodes", "192.168.75.130,192.168.75.129,192.168.75.128");
//        // 端口号
//        params.put("es.port", "9200");
//        // 自动创建索引, 默认true
//        params.put("es.index.auto.create", "false");
//        // 自动发现集群可用节点, 默认true
//        params.put("es.nodes.discovery", "false");
//        // 批处理一次容量, 默认1mb
//        params.put("es.batch.size.bytes", "40mb");
//        // 批处理一次条数, 默认1000
//        params.put("es.batch.size.entries", "40000");
//        // 是否每次bulk操作后都进行refresh。每次refresh后, ES会将当前内存中的数据生成一个新的segment。如果refresh速度过快,
//        // 会产生大量的小segment, 大量segment在进行合并时, 会消耗磁盘的IO, 默认值true。
//        // 在索引的settings中通过refresh_interval配置项进行控制, 可以根据业务的需求设置为30s或更长。
//        params.put("es.batch.write.refresh", "false");
//        // 控制单次批量写入请求的重试次数, 默认值为3, 为了防止集群偶发的网络抖动或压力过大造成的集群短暂熔断, 建议将这个值调大
//        params.put("es.batch.write.retry.count", "50");
//        // 控制单次批量写入请求的重试间隔。
//        params.put("es.batch.write.retry.wait", "10s");
//        // 控制http接口层面的超时, 覆盖读请求和写请求, 默认值1m。建议重试次数50次。
//        params.put("es.http.timeout", "5m");
//        // 控制http接口层面的重试, 覆盖读请求和写请求, 默认值3。建议重试次数50次。
//        params.put("es.http.retries", "50");
//
//        //studentDS.saveToEs("test1/word", params);
//
        sc.close();
    }

}


