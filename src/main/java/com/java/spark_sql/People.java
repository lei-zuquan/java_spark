package com.java.spark_sql;


import java.io.Serializable;

public class People implements Serializable {
    private int id;
    private String name;
    //private int


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
