package com.java.meet;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-16 9:08
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * 一.冒泡排序简介
 *    比较相邻的元素。如果第一个比第二个大，就交换他们两个。
 *    对每一对相邻元素作同样的工作，从开始第一对到结尾的最后一对。在这一点，最后的元素应该会是最大的数。
 *    针对所有的元素重复以上的步骤，除了最后一个。
 *    持续每次对越来越少的元素重复上面的步骤，直到没有任何一对数字需要比较。
 */
public class BubbleSort {
    public static void main(String[] args) {
        int[] sorted = bubbleSort(new int[]{5, 2, 0, 1, 3});
        for (int i : sorted) {
            System.out.print(i + "\t");
        }
        System.out.println("Finished BubbleSort");
    }

    public static int[] bubbleSort(int[] arr) {
        // 外层循环控制比较轮数
        for (int i = 0; i < arr.length; i++) {
            // 内层循环控制每轮比较次数
            for (int j = 0; j < arr.length - i - 1; j++) {
                // 按照从小到大排列
                if (arr[j] > arr[j + 1]) {
                    int temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
        }
        return arr;
    }// bubbleSort
}
