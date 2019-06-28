package com.qiao.test.wordCount;

import java.util.ArrayList;
import java.util.List;

public class Test {


    public static void main(String[] args) {
        List list = new ArrayList();
        for (int i = 0; i < 10000; i++) {
            list.add("name : " + i);
        }
        list.forEach(e -> {
            list.add("123");
        });
    }
}
