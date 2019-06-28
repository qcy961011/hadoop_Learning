package com.qiao.test.test;

public class OtherTake {

    public static int[] productExceptSelf(int[] nums) {
        int key = 1;
        int[] output = new int[nums.length];
        for (int i = 0; i < nums.length; i++) {
            output[i] = key;
            key = nums[i] * key;
        }
        key = 1;
        for (int i = nums.length - 1; i >= 0 ; i--) {
            output[i] = output[i] * key;
            key = nums[i] * key;
        }
        return output;
    }

    public static void main(String[] args) {
        int[] arr = {1, 2, 3, 4, 5};
        int[] result = productExceptSelf(arr);
        for (int i = 0; i < result.length; i++) {
            System.out.printf(result[i] + "\t");
        }
    }
}
