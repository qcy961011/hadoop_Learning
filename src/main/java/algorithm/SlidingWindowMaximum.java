package algorithm;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * 滑动窗口最大值
 */
public class SlidingWindowMaximum {
    public static void main(String[] args) {
        int[] nums = {1, 3, -1, -3, 5, 3, 6, 7};
        int[] ints = maxSlidingWindowDeq(nums, 3);
        for (int i = 0; i < ints.length; i++) {
            System.out.println(ints[i]);
        }

    }

    /**
     * 暴力选举
     */
    public static int[] maxSlidingWindowViolence(int[] nums, int k) {
        int n = nums.length;
        if (n * k == 0) return new int[0];

        int[] output = new int[n - k + 1];
        for (int i = 0; i < n - k + 1; i++) {
            int max = Integer.MIN_VALUE;
            for (int j = i; j < (i + k); j++) {
                max = Math.max(nums[j], max);
            }
            output[i] = max;
        }

        return output;
    }

    /**
     * 使用大顶堆优先队列
     */
    private static PriorityQueue<Integer> priorityQueue = new PriorityQueue<>(11, new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o2 - o1;
        }
    });

    public static int[] maxSlidingWindowMaxProQueue(int[] nums, int k) {
        int n = nums.length;
        if (n * k == 0) return new int[0];

        int[] output = new int[n - k + 1];
        for (int i = 0; i < n - k + 1; i++) {
            for (int j = i; j < (i + k); j++) {
                priorityQueue.offer(nums[j]);
            }
            output[i] = priorityQueue.peek();
        }

        return output;
    }

    /**
     * 使用双向队列
     */
    private static ArrayDeque<Integer> deq = new ArrayDeque<>();

    public static void clean_deque(int i, int k, int[] nums) {
        // remove indexes of elements not from sliding window
        // 删除下标不在滑动窗口中的元素
        if (!deq.isEmpty() && deq.getFirst() == i - k)
            deq.removeFirst();

        // remove from deq indexes of all elements
        // which are smaller than current element nums[i]
        // 删除所有小于值小于当前进入的下标的元素
        while (!deq.isEmpty() && nums[i] > nums[deq.getLast()]) {
            deq.removeLast();
        }
    }

    public static int[] maxSlidingWindowDeq(int[] nums, int k) {
        int n = nums.length;
        if (n * k == 0) return new int[0];
        if (k == 1) return nums;

        // init deque and output
        int max_idx = 0;
        for (int i = 0; i < k; i++) {
            clean_deque(i, k, nums);
            deq.addLast(i);
            // compute max in nums[:k]
            if (nums[i] > nums[max_idx]) max_idx = i;
        }
        int[] output = new int[n - k + 1];
        output[0] = nums[max_idx];

        // build output
        for (int i = k; i < n; i++) {
            clean_deque(i, k, nums);
            deq.addLast(i);
            output[i - k + 1] = nums[deq.getFirst()];
        }
        return output;
    }

}
