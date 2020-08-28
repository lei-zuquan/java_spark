//package com.java.spark_core.file_read_save;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.streaming.Duration;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka.KafkaUtils;
//import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
//import scala.Tuple2;
//
//import java.text.SimpleDateFormat;
//import java.util.Arrays;
//import java.util.Date;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.regex.Pattern;
//
///**
// * @Author:
// * @Date: 2020-08-28 19:10
// * @Version: 1.0
// * @Modified By:
// * @Description:
// */
//public class Java_Spark21_SparkToES {
//    public static void main(String[] args) throws InterruptedException {
//        //设置匹配模式，以空格分隔
//        final Pattern SPACE = Pattern.compile(" ");
//
//        //接收数据的地址和端口
//        String zkQuorum = "t-kafka1.hdp.mobike.cn:2181";
//        //话题所在的组
//        String group = "1";
//        //话题名称以“，”分隔
//        String topics = "api_newlog";
//        //每个话题的分片数
//        int numThreads = 1;
//
//        SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount");
//        sparkConf.set("es.index.auto.create", "true");
//        sparkConf.set("es.nodes", "10.3.3.83");
//        //---->如果是连接的远程es节点，该项必须要设置
//        sparkConf.set("es.port", "9200");
//        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(4000));
//        //jssc.checkpoint("hdfs://mycluster/tmp/"+yyyyerr+"/"+mmerr+"/"+dderr+"/"+hherr+"/"); //设置检查点
//        //存放话题跟分片的映射关系
//        Map<String, Integer> topicmap = new HashMap<String, Integer>();
//        String[] topicsArr = topics.split(",");
//        int n = topicsArr.length;
//        for (int i = 0; i < n; i++) {
//            topicmap.put(topicsArr[i], numThreads);
//        }
//        //从Kafka中获取数据转换成RDD
//        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jssc, zkQuorum, group, topicmap);
//        //从话题中过滤所需数据
//        JavaDStream<String> nn = lines.map(new Function<Tuple2<String, String>, String>() {
//            public String call(Tuple2<String, String> tuple2) {
//                return tuple2._2();
//            }
//        });
//        JavaDStream<String> a = nn.flatMap(new FlatMapFunction<String, String>() {
//
//            public Iterable<String> call(String arg0) throws Exception {
//                // TODO Auto-generated method stub
//                Map<String, Object> map = new HashMap();
//                map.put("comments", arg0);
//                Date date = new Date();
//                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
//                String dateF = dateFormat.format(date);
//
//                map.put("commentDate", dateF);
//                return Arrays.asList(map);
//            }
//        });
//        a.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//            public void call(JavaRDD<String> t) throws Exception {
//                JavaEsSpark.saveToEs(t, "logs/docs");
//            }
//        });
//        a.print();
//        jssc.start();
//        jssc.awaitTermination();
//    }
//}
