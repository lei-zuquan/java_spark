package com.java.serializable;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:37 上午 2020/5/10
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

import java.io.Serializable;
import java.util.Map;

public class Simple implements Serializable {
    private static final long serialVersionUID = -4914434736682797743L;
    private String name;
    private int age;
    private Map<String, Integer> map;

    public Simple() {

    }

    public Simple(String name, int age, Map<String, Integer> map) {
        this.name = name;
        this.age = age;
        this.map = map;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public Map<String, Integer> getMap() {
        return map;
    }

    public void setMap(Map<String, Integer> map) {
        this.map = map;
    }


}