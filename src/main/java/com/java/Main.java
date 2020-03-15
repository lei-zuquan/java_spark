package com.java;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/*
 * 2020 03 15 某通信公司大厂笔试题
 */
public class Main {
    /**
     * 输入n个数字，输入获取第K个，返回排列的第k个即可。
     * 如:输入n为3，K=3
     * 123
     * 132
     * 213
     * 231
     * 312
     * 321
     * 则结果返回：213
     */
    public static void main(String[] args) {
        Date startTime = new Date();
        int totalCount = 6;
        int getIndex = 2;
        String result = getNumListByIndex(totalCount, getIndex);
        System.out.println(result);

        Date endTime = new Date();
        long spendTime = endTime.getTime() - startTime.getTime();
        System.out.println("消耗时间为：" + spendTime + " 毫秒");
    }

    public static String getNumListByIndex(int n, int k) {
        int totalCnt = getTotalCount(n);
        if (k > totalCnt) {
            return "输入：" + n + " 可产生：" + totalCnt + " 种不同的组合\n，您输入的K超过：" + totalCnt + "\n无法给你返回！";
        }

        List<String> listValue = getTotalListValue(n);
        System.out.println("=====================");
        listValue.forEach(System.out::println);
        System.out.println("=====================");

        return listValue.get(k - 1);
    }

    // 获取总共有多少种排列组合：1*2*3*4*...*n种
    public static int getTotalCount(int n) {

        int total = 1;
        for (int i = 1; i <= n; i++) {
            total = total * i;
        }
        return total;
    }

    public static List<String> getTotalListValue(int totalCount) {
        // 获取总共有多少种数字
        List<Integer> totalTotalChar = getTotalTotalChar(totalCount);

        List<String> listValue = new ArrayList<>();
        for (int i = 1; i <= totalCount; i++) {
            listValue = getBaseList(totalTotalChar);

        }

        return listValue;
    }

    public static List<String> getBaseList(List<Integer> list) {
        List<String> returnLit = new ArrayList<>();
        // 如果超过2个数字，则继续调自己

        if (list.size() > 2) {
            for (int i = 0; i < list.size(); i++) {
                String head = list.get(i) + "";
                List<Integer> copy = new ArrayList<>();
                for (int j = 0; j < list.size(); j++) {
                    if (j != i) {
                        copy.add(list.get(j));
                    }
                }

                List<String> baseListValue = getBaseList(copy);
                for (int k = 0; k < baseListValue.size(); k++) {
                    returnLit.add(head + baseListValue.get(k));
                }

            }

        } else {
            if (list.size() == 2) {
                returnLit.add(list.get(0) + "" + list.get(1));
                returnLit.add(list.get(1) + "" + list.get(0));
            } else {
                returnLit.add(list.get(0) + "");
            }

        }

        return returnLit;
    }

    // 获取总共有多少种数字
    public static List<Integer> getTotalTotalChar(int totalCount) {
        List<Integer> list = new ArrayList<>();

        for (int i = 1; i <= totalCount; i++) {
            list.add(i);
        }

        return list;
    }
}
