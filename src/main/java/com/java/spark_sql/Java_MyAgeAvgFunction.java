package com.java.spark_sql;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.LongType;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-09 12:59
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class Java_MyAgeAvgFunction extends UserDefinedAggregateFunction {
    @Override
    public StructType inputSchema() {
        return new StructType().add("age", LongType);
    }

    @Override
    public StructType bufferSchema() {
        return new StructType().add("sum",LongType).add("count", LongType);
    }

    @Override
    public DataType dataType() {
        return DoubleType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        // 没有名称，只有结构，只能通过标记位来确定
        buffer.update(0,0L);
        buffer.update(1,0L);
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        buffer.update(0, buffer.getLong(0) + input.getLong(0));
        buffer.update(1, buffer.getLong(1) +1);
    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        //sum
        buffer1.update(0, buffer1.getLong(0)+buffer2.getLong(0));
        buffer1.update(1, buffer1.getLong(1)+buffer2.getLong(1));
    }

    @Override
    public Object evaluate(Row buffer) {
        return buffer.getLong(0) / buffer.getLong(1);
    }
}
