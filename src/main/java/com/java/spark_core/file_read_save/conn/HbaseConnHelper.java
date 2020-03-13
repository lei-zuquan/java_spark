package com.java.spark_core.file_read_save.conn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HbaseConnHelper {

    // DCL机制 + volatile禁止指令重排
    // 与HBase数据库的连接对象
    private static volatile Connection connection = null;


    /**
     * 私有化构造方法，让用户不能new这个类的对象
     */
    private HbaseConnHelper() {

    }

    /**
     * 创建连接对象
     *
     * @return
     */
    private static Connection createConnection() {
        Configuration conf = new Configuration();

        /**
         * 连接hbase有两种方式：
         * 方式一：
         * 		Configuration.set("hbase.zookeeper.quorum", zk_list);
         * 		Configuration..set("hbase.zookeeper.property.clientPort", "2181");
         * 方式二：
         * 		hbase-default.xml,hbase-site.xml
         */
        String zk_list = "node-03,node-01,node-02";
        conf.set("hbase.zookeeper.quorum", zk_list);
        // 设置连接参数:hbase数据库使用的接口
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        //conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        //conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

        //ExecutorService pool = Executors.newFixedThreadPool(10);//建立一个数量为10的线程池
        try {
            //connection = ConnectionFactory.createConnection(conf, pool);//用线程池创建connection
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return connection;
    }


    /**
     * 获取连接对象
     *
     * @return
     */
    public static Connection getConnection() {
        if (null == connection) {
            synchronized (HbaseConnHelper.class) {
                if (null == connection) {//空的时候创建，不为空就直接返回；典型的单例模式
                    connection = createConnection();
                }
            }
        }
        return connection;
    }

    /**
     * 关闭连接
     */
    public static void closeConnection() {
        try {
            if (connection != null) {
                connection.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取数据库元数操作对象
     *
     * @return
     */
    public static Admin getHAdmin() {
        Admin hadmin = null;
        try {
            hadmin = getConnection().getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hadmin;
    }

    /**
     * 关闭数据库元数操作对象
     *
     * @param hadmin
     */
    public static void closeHAdmin(Admin hadmin) {
        if (hadmin != null) {
            try {
                hadmin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 通过表名，获取表对象
     *
     * @param tableName
     * @return
     */
    public static Table getTable(String tableName) {
        Table table = null;

        try {
            table = getConnection().getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return table;
    }

    public static void closeTable(Table table) {
        if (table != null) {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
} 