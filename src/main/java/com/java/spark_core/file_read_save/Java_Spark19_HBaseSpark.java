package com.java.spark_core.file_read_save;

import com.java.spark_core.file_read_save.util.HBaseToolUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-13 10:29
 * @Version: 1.0
 * @Modified By:
 * @Description: Spark core 操作Hbase 示例
 */

/*
由于 org.apache.hadoop.hbase.mapreduce.TableInputFormat 类的实现，Spark 可以通过Hadoop输入格式访问HBase。
这个输入格式会返回键值对数据，其中键的类型为org. apache.hadoop.hbase.io.ImmutableBytesWritable，
而值的类型为org.apache.hadoop.hbase.client.Result。
 */
public class Java_Spark19_HBaseSpark {
    public static void main(String[] args) throws Exception {
//        readDataFromHbase();

        Date startTime;
        startTime = new Date();
        HBaseToolUtil.truncateTable("spark_learn_student");

        showSpendTime(startTime);
        startTime = new Date();
        saveDataToHbase(10000);
        showSpendTime(startTime);
        startTime = new Date();
    }

    private static void saveDataToHbase(int recordCount) {
        SparkConf config = new SparkConf();
        config.setAppName("Java_Spark19_HBaseSpark").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(config);

        List<Tuple2<Integer, String>> list = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            list.add(new Tuple2<>(i, "student:" + i));
        }

        JavaPairRDD<Integer, String> rdd = jsc.parallelizePairs(list, 2);
        rdd.foreachPartition(iterator -> {
            List<Put> puts = new ArrayList<Put>();
            byte[] family = "info".getBytes();
            byte[] colName = "name".getBytes();

            while (iterator.hasNext()) {
                Tuple2<Integer, String> next = iterator.next();

                String rowKey = next._2;
                Put put = new Put(rowKey.getBytes());
                //put.addColumn("info".getBytes(), "name".getBytes(), next._2.getBytes());
                put.addColumn(family, colName, next._2.getBytes());

                puts.add(put);

                if (puts.size() >= 10000) {
                    HBaseToolUtil.savePutList(puts, "spark_learn_student");

                    puts.clear();
                }
            }
            HBaseToolUtil.savePutList(puts, "spark_learn_student");
        });


        jsc.close();
    }

    private static void readDataFromHbase() {
        SparkConf config = new SparkConf();
        config.setAppName("Java_Spark19_HBaseSpark").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(config);

        //构建HBase配置信息
        Configuration hconf = HBaseConfiguration.create();

        String zk_list = "node-03,node-01,node-02";
        hconf.set("hbase.zookeeper.quorum", zk_list);
        // 设置连接参数:hbase数据库使用的接口
        hconf.set("hbase.zookeeper.property.clientPort", "2181");

        /*
        hbase shell:进入hbase客户端创建表

        create 'spark_learn_student','info'
        put 'spark_learn_student','1001','info:name','zhangsan'
         */
        String tableName = "spark_learn_student";
        String FAMILY = "info";
        String COLUM_NAME = "name";
        hconf.set(TableInputFormat.INPUT_TABLE, tableName);

        //从HBase读取数据形成RDD
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY));
        scan.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_NAME));

        try {
            //添加scan
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String ScanToString = Base64.encodeBytes(proto.toByteArray());
            hconf.set(TableInputFormat.SCAN, ScanToString);

            //读HBase数据转化成RDD
            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = jsc.newAPIHadoopRDD(hconf,
                    TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            hbaseRDD.cache();// 对myRDD进行缓存
            System.out.println("数据总条数：" + hbaseRDD.count());

            //将Hbase数据转换成PairRDD，年龄：姓名
            JavaPairRDD<String, String> mapToPair = hbaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable,
                    Result>, String, String>() {
                private static final long serialVersionUID = -2437063503351644147L;

                @Override
                public Tuple2<String, String> call(
                        Tuple2<ImmutableBytesWritable, Result> resultTuple2) throws Exception {
                    byte[] o1 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_NAME));//取列的值
                    //byte[] o2 = resultTuple2._2.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUM_AGE));//取列的值
                    return new Tuple2<String, String>(Bytes.toString(o1), Bytes.toString(o1));
                }
            });

            //按年龄降序排序
            JavaPairRDD<String, String> sortByKey = mapToPair.sortByKey(false);
            sortByKey.foreach(tuple -> {
                System.out.println(tuple._1);
            });
            //写入数据到hdfs系统
            //sortByKey.saveAsTextFile("hdfs://********:8020/tmp/test");
            //hbaseRDD.unpersist();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }


        jsc.close();
    }


    private static void showSpendTime(Date startTime) {
        Date endTime = new Date();
        long spendTime = endTime.getTime() - startTime.getTime();
        System.out.println("spendTime:" + spendTime / 1000 + " 秒");
    }
}


