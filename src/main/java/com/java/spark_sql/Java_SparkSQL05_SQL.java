package com.java.spark_sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:31 下午 2020/3/9
 * @Version: 1.0
 * @Modified By:
 * @Description: sparkSQL从文件加载数据注册表进行数据查询示例
 */
public class Java_SparkSQL05_SQL {
    public static void main(String[] args) {
        //使用Hive的操作
        //val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath //如果是内置的、需指定hive仓库地址，若使用的是外部Hive，则需要将hive-site.xml添加到ClassPath下。
        SparkSession session = SparkSession.builder()
                .appName("Spark Hive Example")
                .master("local[*]")
                // .config("spark.sql.warehouse.dir", warehouseLocation) // 如果使用spark默认hive数仓，则需要此配置
                //      .enableHiveSupport()
                .getOrCreate();

        JavaRDD<String> rdd = session.sparkContext().textFile("in/person.txt", 2).toJavaRDD();
        //var frame: DataFrame = session.read.json("in/user.json")

        //整理数据，ROW类型
        JavaRDD<Row> rowRDD = rdd.map(line -> {
            String[] fields = line.split(",");
            Row row = RowFactory.create(
                    Integer.valueOf(fields[0]),
                    fields[1],
                    Integer.valueOf(fields[2]),
                    Long.valueOf(fields[3])
            );
            return row;
        });

        //scheme:定义DataFrame里面元素的数据类型，以及对每个元素的约束
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("faceValue", DataTypes.LongType, true));
        StructType structType = DataTypes.createStructType(structFields);

        // 将rowrdd和structType关联,因为文本 类 没有结构，所以
        //val df:DataFrame = session.createDataFrame(rowrdd, structType)
        Dataset<Row> df = session.createDataFrame(rowRDD, structType);
        // 创建一个视图
        df.createOrReplaceTempView("Person");
        // 基于注册的视图写SQL
        Dataset<Row> res = session.sql("SELECT id, name, age, faceValue FROM Person ORDER BY age asc");

        res.show();

        // 释放资源
        session.stop();
    }
}
