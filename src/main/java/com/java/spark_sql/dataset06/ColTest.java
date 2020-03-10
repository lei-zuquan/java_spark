package com.java.spark_sql.dataset06;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-10 14:57
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class ColTest {
    private String name;
    private Integer id;

    public ColTest() {
    }

    public ColTest(String name, Integer id) {
        this.name = name;
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }
}
