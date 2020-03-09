package com.java.spark_sql.udaf04;

import java.io.Serializable;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-09 14:11
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

public class UserBean implements Serializable {
    private String name;
    private Long age;

    public UserBean(String name, Long age) {
        this.name = name;
        this.age = age;
    }

    public UserBean() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getAge() {
        return age;
    }

    public void setAge(Long age) {
        this.age = age;
    }
}
