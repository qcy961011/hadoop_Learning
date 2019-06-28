package com.qiao.test.test;

public class IntegerText {

    public static void main(String[] args) {
//        Integer a = 128;
//        Integer b = 128;
//        Integer c = 127;
//        Integer d = 127;
//        System.out.println(a == b);
//        System.out.println(c == d);

        System.out.println(test());
    }

    public static int test(){
        try {
            return 2;
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            return 1;
        }
    }
}
