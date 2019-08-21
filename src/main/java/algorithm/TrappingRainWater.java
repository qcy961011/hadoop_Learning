package algorithm;

import java.util.Stack;

/**
 * 接雨水
 * 共有五种解题方式
 */
public class TrappingRainWater {

    public static void main(String[] args) {
        int[] height = {0, 1, 0, 2, 1, 0, 1, 3, 2, 1, 2, 1};
//        int[] height = {2, 1, 0, 2};
        System.out.println(trapStack(height));
    }


    /**
     * 按行扫描
     * 有洼地计数
     */
    private static int trapLine(int[] height) {
        int sum = 0;
        int max = getMax(height);//找到最大的高度，以便遍历。
        for (int i = 1; i <= max; i++) {
            boolean isStart = false; //标记是否开始更新 temp
            int temp_sum = 0;
            for (int j : height) {
                // 有小值（洼地）开始计数
                if (isStart && j < i) {
                    temp_sum++;
                }
                // 有峰值（高地）开始计数
                if (j >= i) {
                    sum = sum + temp_sum;
                    temp_sum = 0;
                    isStart = true;
                }
            }
        }
        return sum;
    }

    private static int getMax(int[] height) {
        int max = 0;
        for (int i : height) {
            if (i > max) {
                max = i;
            }
        }
        return max;
    }

    /**
     * 压栈方式计算
     */
    private static int trapStack(int[] height) {
        int sum = 0;
        Stack<Integer> stack = new Stack<>();
        for (int i = 0; i < height.length; i++) {
            while (!stack.empty() && height[stack.peek()] < height[i]) {
                int high = height[stack.peek()];
                stack.pop();
                if (stack.empty()){
                    break;
                }
                int distance = i - stack.peek() - 1;
                int min = Math.min(height[stack.peek()] , height[i]);
                sum += distance * (min - high);
            }
            stack.push(i);
        }
        return sum;
    }
}