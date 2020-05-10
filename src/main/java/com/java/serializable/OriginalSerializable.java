package com.java.serializable;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:29 上午 2020/5/10
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

import java.io.*;
import java.util.HashMap;
import java.util.Map;


public class OriginalSerializable {

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        setSerializableObject();
        System.out.println("java原生序列化时间:" + (System.currentTimeMillis() - start) + " ms");
        start = System.currentTimeMillis();
        getSerializableObject();
        System.out.println("java原生反序列化时间:" + (System.currentTimeMillis() - start) + " ms");
    }

    public static void setSerializableObject() throws IOException {

        FileOutputStream fo = new FileOutputStream("out/OriginalSerializable.bin");

        ObjectOutputStream so = new ObjectOutputStream(fo);

        for (int i = 0; i < 100000; i++) {
            Map<String, Integer> map = new HashMap<>(2);
            map.put("zhang0", i);
            map.put("zhang1", i);
            so.writeObject(new Simple("zhang" + i, (i + 1), map));
        }
        so.flush();
        so.close();
    }

    public static void getSerializableObject() {
        FileInputStream fi;
        try {
            fi = new FileInputStream("out/OriginalSerializable.bin");
            ObjectInputStream si = new ObjectInputStream(fi);

            Simple simple = null;
            while ((simple = (Simple) si.readObject()) != null) {
                //System.out.println(simple.getAge() + "  " + simple.getName());
            }
            fi.close();
            si.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            //e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
