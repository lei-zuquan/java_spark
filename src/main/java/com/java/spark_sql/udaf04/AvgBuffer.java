package com.java.spark_sql.udaf04;

import java.io.Serializable;

public class AvgBuffer implements Serializable {
    private Long sum;
    private int count;

    public AvgBuffer(Long sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public AvgBuffer() {
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
