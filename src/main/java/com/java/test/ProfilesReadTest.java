package com.java.test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-09 15:18
 * @Version: 1.0
 * @Modified By:
 * @Description: 测试Idea中Profiles编译选项
 */

/*
 *         <!-- 导入加载配置文件的依赖-->
 *         <dependency>
 *             <groupId>com.typesafe</groupId>
 *             <artifactId>config</artifactId>
 *             <version>1.2.1</version>
 *         </dependency>
 *
 */
public class ProfilesReadTest {
    public static void main(String[] args) {
        // 1.需要在pom.xml添加maven依赖

        // 加载配置
        Config conf = ConfigFactory.load();

        String value = conf.getString("run.on.test");
        System.out.println(value);
    }
}
