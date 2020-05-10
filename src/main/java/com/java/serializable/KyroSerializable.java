package com.java.serializable;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:39 上午 2020/5/10
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class KyroSerializable {

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        setSerializableObject();
        System.out.println("Kryo 序列化时间:" + (System.currentTimeMillis() - start) + " ms");
        start = System.currentTimeMillis();
        getSerializableObject();
        System.out.println("Kryo 反序列化时间:" + (System.currentTimeMillis() - start) + " ms");

    }

    public static void setSerializableObject() throws FileNotFoundException {

        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.register(Simple.class);
        Output output = new Output(new FileOutputStream("out/KyroSerializable.bin"));
        for (int i = 0; i < 100000; i++) {
            Map<String, Integer> map = new HashMap<>(2);
            map.put("zhang0", i);
            map.put("zhang1", i);
            kryo.writeObject(output, new Simple("zhang" + i, (i + 1), map));
        }
        output.flush();
        output.close();
    }


    public static void getSerializableObject() {
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        Input input;
        try {
            input = new Input(new FileInputStream("out/KyroSerializable.bin"));
            Simple simple = null;
            while ((simple = kryo.readObject(input, Simple.class)) != null) {
                //System.out.println(simple.getAge() + "  " + simple.getName() + "  " + simple.getMap().toString());
            }

            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (KryoException e) {

        }
    }

}