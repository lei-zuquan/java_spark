package com.java.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;

import java.util.List;
import java.util.Map;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-11 13:36
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class DefineMyKafkaPartitioner implements Partitioner {
//  private static Logger logger = Logger.getLogger(MyLogPartitioner.class);

    @Override
    public void configure(Map<String, ?> configs) {
        // TODO Auto-generated method stub

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        /**
         *由于我们按key分区，在这里我们规定：key值不允许为null。在实际项目中，key为null的消息*，可以发送到同一个分区。
         */
        if (keyBytes == null) {
            throw new InvalidRecordException("key cannot be null");
        }
        if ("-1".equals((String) key)) {
            return 1;
        }
        //如果消息的key值不为-1，那么使用key取模，确定分区。
        int keyValue = Integer.parseInt((String) key);
        return keyValue % numPartitions;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}