package com.qiao.test.test;

public class DoubleArr {

    public static int[][] flipAndInvertImage(int[][] A) {

        int[][] output = new int[A.length][];

        for (int i = 0; i < A.length; i++) {
            output[i] = reverse(horizontalFlip(A[i]));
        }

        return output;
    }


    public static int[] horizontalFlip(int[] arr) {
        int[] output = new int[arr.length];
        for (int i = 0; i < arr.length; i++) {
            output[arr.length - 1 - i] = arr[i];
        }
        return output;
    }

    public static int[] reverse(int[] arr) {
        int[] output = new int[arr.length];
        for (int i = 0; i < arr.length; i++) {
            output[i] = ~arr[i];
        }
        return output;
    }

}
