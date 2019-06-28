package com.qiao.test.test;

public class Test {

    public static void main(String[] args) {
        int[] arr = {-4, -1, 0, 3, 10};
        int tmp;
        for (int i = 0; i < arr.length; i++) {
            arr[i] = (int)Math.pow(arr[i] , 2);
        }

        sort(arr , 0 , arr.length - 1);
        for (int i :
             arr) {
            System.out.printf(i + "\t");
        }
    }

    public static void sort(int[] arr , int low , int high){
        int start = low;
        int end = high;
        int key = arr[low];
        while (start < end) {
            while (start < end && key >= arr[end]) {
                end--;
            }
            if(key < arr[end]) {
                int tem = arr[end];
                arr[end] = arr[start];
                arr[start] = tem;
            }
            while (start < end && key <= arr[start]) {
                start++;
            }
            if(key > arr[start]) {
                int tem = arr[end];
                arr[end] = arr[start];
                arr[start] = tem;
            }
        }
        if(start > low) sort(arr , low , start -1);
        if(end < high) sort(arr , end + 1 , high);
    }

}
