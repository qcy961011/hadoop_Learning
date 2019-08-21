package algorithm;

import java.util.PriorityQueue;

/**
 * 数据流中的第K大元素
 */
public class KthLargestElementInStream {

    private PriorityQueue<Integer> minHeap = null;

    private int k = 0;

    public KthLargestElementInStream(int k , int[] nums) {
        this.k = k;
        this.minHeap = new PriorityQueue<>(k);

        for (int i = 0; i < nums.length; i++) {
            add(nums[i]);
        }
    }

    public int add(int val) {
        if (minHeap.size() < k) {
            minHeap.offer(val);
        } else if (minHeap.peek() < val) {
            minHeap.poll();
            minHeap.offer(val);
        }
        return minHeap.peek();
    }

}
