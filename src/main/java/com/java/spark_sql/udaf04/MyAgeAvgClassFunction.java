package com.java.spark_sql.udaf04;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

//这里输入 数据类型 需要改为BigInt，不能为Int。因为程序读取文件的时候，不能判断int类型到底多大，所以会报错 truncate

// 声明用户自定义聚合函数(强类型)
// 1, 继承：Aggregator,设定泛型
// 2,实现方法
public class MyAgeAvgClassFunction extends Aggregator<UserBean, AvgBuffer, Double> {

    //初始化
    @Override
    public AvgBuffer zero() {
        return new AvgBuffer(0L, 0);
    }

    //聚合数据
    @Override
    public AvgBuffer reduce(AvgBuffer b, UserBean a) {
        b.setSum(b.getSum() + a.getAge());
        b.setCount(b.getCount() + 1);

        return b;
    }

    //缓冲区的合并操作
    @Override
    public AvgBuffer merge(AvgBuffer b1, AvgBuffer b2) {
        b1.setSum(b1.getSum() + b2.getSum());
        b1.setCount(b1.getCount() + b2.getCount());

        return b1;
    }

    //完成计算
    @Override
    public Double finish(AvgBuffer reduction) {
        double v = reduction.getSum() * 1.0 / reduction.getCount();
        return v;
    }

    //数据类型转码，自定义类型 基本都是Encoders.product
    @Override
    public Encoder<AvgBuffer> bufferEncoder() {
        return Encoders.bean(AvgBuffer.class);
    }

    @Override
    public Encoder<Double> outputEncoder() {
        return Encoders.DOUBLE();
    }
}
