package com.qiao.test.test;

public class IntArr {
    public static void main(String[] args) {
        String S = "IDID";
        int[] arr = new int[S.length() + 1];
        char[] chars = S.toCharArray();
        int max = S.length();
        int min = 0;
        for(int i = 0 ; i < chars.length; i++) {
            if (chars[i] == 'I'){
                arr[i] = min;
                min++;
            } else if (chars[i] == 'D'){
                arr[i] = max;
                max--;
            }
        }
        arr[arr.length - 1] = max;
        System.out.println(arr);
    }
}
