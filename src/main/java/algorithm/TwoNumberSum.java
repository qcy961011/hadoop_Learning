package algorithm;

import java.util.HashMap;
import java.util.Map;

public class TwoNumberSum {

    public static void main(String[] args) {
        int arr[] = {2, 7, 5, 6, 3};
        int[] ints = twoSumTwo(arr, 9);
        for (int i = 0; i < ints.length; i++) {
            System.out.println(ints[i]);
        }
    }

    /**
     * 初始化Map的方法
     */
    public static int[] twoSum(int[] arr, int targe) {
        int[] res = new int[2];
        Map<Integer, Integer> map = new HashMap();
        for (int i = 0; i < arr.length; i++) {
            map.put(arr[i], i);
        }
        for (int i = 0; i < arr.length; i++) {
            if (map.containsKey(targe - arr[i])) {
                res[0] = i;
            }
            res[1] = map.get(targe - arr[i]);
            return res;
        }
        return res;
    }

    public static int[] twoSumTwo(int[] arr, int targe) {
        int[] res = new int[2];
        Map<Integer, Integer> map = new HashMap();
        for (int i = 0; i < arr.length; i++) {
            if (map.containsKey(targe - arr[i])) {
                res[0] = i;
                res[1] = map.get(targe - arr[i]);
                return res;
            } else {
                map.put(arr[i] , i);
            }
        }
        return res;
    }


}
