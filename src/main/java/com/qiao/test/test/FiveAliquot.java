package com.qiao.test.test;

import java.util.ArrayList;
import java.util.List;

public class FiveAliquot {

    public static List<Boolean> fiveAliquot(int[] arr) {
        List<Boolean> booList = new ArrayList<>(arr.length);

        int falg = 0;
        for (int i = 0; i < arr.length; i++) {
            falg = (falg << 1) + arr[i];
            if (falg > 10) {
                falg = falg % 10;
            }
            if (falg % 5 == 0) {
                booList.add(true);
            } else {
                booList.add(false);
            }
        }
        return booList;
    }

    public static void main(String[] args) {
        int[] i = new int[]{1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 1};
        List<Boolean> booleans = fiveAliquot(i);
        booleans.forEach(boo -> {
            System.out.println(boo);
        });
    }
}
