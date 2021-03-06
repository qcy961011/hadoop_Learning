package com.util;


import java.util.ArrayList;
import java.util.List;

/**
 * 用于排序的工具类
 *
 * @author Qiao
 *
 */
public class SortUtil {
    /**
     * 插入排序 算法时间复杂度O(n2)--[n的平方] 在要排序的一组数中，假设前面(n-1)[n>=2] 个数已经是排 好顺序的，现在要把第n
     * 个数插到前面的有序数中，使得这 n个数 也是排好顺序的。如此反复循环，直到全部排好顺序。
     *
     * @param arr
     * @return
     */
    public static int[] insertSort(int arr[]) {
        int temp = 0;
        for (int i = 1; i < arr.length; i++) {
            int j = i - 1;
            temp = arr[i];
            for (; j >= 0 && temp < arr[j]; j--) {
                arr[j + 1] = arr[j]; // 将大于temp 的值整体后移一个单位
            }
            arr[j + 1] = temp;
        }
        return arr;
    }

    /**
     * 希尔排序 基本思想：算法先将要排序的一组数按某个增量 d（n/2,n为要排序数的个数）分成若
     *
     * 干组，每组中记录的下标相差 d.对每组中全部元素进行直接插入排序，然后再用一个较小
     *
     * 的增量（d/2）对它进行分组，在每组中再进行直接插入排序。当增量减到 1 时，进行直接
     *
     * 插入排序后，排序完成。
     *
     * @param arr
     * @return
     */
    public static int[] shellSort(int arr[]) {
        double d1 = arr.length;
        int temp = 0;
        while (true) {
            d1 = Math.ceil(d1 / 2);
            int d = (int) d1;
            for (int x = 0; x < d; x++) {
                for (int i = x + d; i < arr.length; i += d) {
                    int j = i - d;
                    temp = arr[i];
                    for (; j >= 0 && temp < arr[j]; j -= d) {
                        arr[j + d] = arr[j];
                    }
                    arr[j + d] = temp;
                }
            }
            if (d == 1) {
                break;
            }
        }
        return arr;
    }

    /**
     * 冒泡排序 算法时间复杂度O(n2)--[n的平方] 基本思想：在要排序的一组数中，对当前还未排好序的范围内的全部数，自上而下对
     * 相邻的两个数依次进行比较和调整，让较大的数往下沉，较小的往上冒。即：每当两 相邻的 数比较后发现它们的排序与排序要求相反时，就将它们互换。
     *
     * @param arr
     * @return
     */
    public static int[] bubbleSort(int arr[]) {
        int temp = 0;
        for (int i = 0; i < arr.length - 1; i++) {
            for (int j = 0; j < arr.length - 1 - i; j++) {
                if (arr[j] > arr[j + 1]) {
                    temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
        }
        return arr;
    }

    /**
     * 选择排序 选择排序是不稳定的。算法复杂度O(n2)--[n的平方] 基本思想：在要排序的一组数中，选出最小的一个数与第一个位置的数交换；
     *
     * 然后在剩下的数当中再找最小的与第二个位置的数交换，如此循环到倒数第二个数和最后一
     *
     * 个数比较为止。
     *
     * @param arr
     * @return
     */
    public static int[] selectSort(int arr[]) {
        int position = 0;
        for (int i = 0; i < arr.length; i++) {
            int j = i + 1;
            position = i;
            int temp = arr[i];
            for (; j < arr.length; j++) {
                if (arr[j] < temp) {
                    temp = arr[j];
                    position = j;
                }
            }
            arr[position] = arr[i];
            arr[i] = temp;
        }
        return arr;
    }

    /**
     * 快速排序 快速排序是不稳定的。最理想情况算法时间复杂度O(nlog2n)，最坏O(n2)
     * 基本思想：选择一个基准元素,通常选择第一个元素或者最后一个元素,通过一趟扫描，
     *
     * 将待排序列分成两部分,一部分比基准元素小,一部分大于等于基准元素,此时基准元素在其
     *
     * 排好序后的正确位置,然后再用同样的方法递归地排序划分的两部分。
     *
     * @param arr
     * @return
     */
    public static int[] quickSort(int arr[]) {
        quick(arr);
        return arr;
    }

    /**
     * 快速排序用到的方法
     *
     * @param list
     * @param low
     * @param high
     * @return
     */
    private static int getMiddle(int[] list, int low, int high) {
        int tmp = list[low]; // 数组的第一个作为中轴
        while (low < high) {
            while (low < high && list[high] >= tmp) {
                high--;
            }
            list[low] = list[high]; // 比中轴小的记录移到低端
            while (low < high && list[low] <= tmp) {
                low++;
            }
            list[high] = list[low]; // 比中轴大的记录移到高端
        }
        list[low] = tmp; // 中轴记录到尾
        return low; // 返回中轴的位置
    }

    /**
     * 快速排序用到的方法
     *
     * @param list
     * @param low
     * @param high
     */
    private static void _quickSort(int[] list, int low, int high) {
        if (low < high) {
            int middle = getMiddle(list, low, high); // 将list 数组进行一分 为二
            _quickSort(list, low, middle - 1); // 对低字表进行递归排序
            _quickSort(list, middle + 1, high); // 对高字表进行递归排序
        }
    }

    /**
     * 快速排序用到的方法
     *
     * @param a2
     */
    private static void quick(int[] a2) {
        if (a2.length > 0) { // 查看数组是否为空
            _quickSort(a2, 0, a2.length - 1);
        }
    }

    /**
     * 堆排序 基本思想：堆排序是一种树形选择排序，是对直接选择排序的有效改进。
     *
     * 堆的定义如下：具有n个元素的序列（h1,h2,...,hn),当且仅当满足（hi>=h2i,hi>=2i+1）或
     *
     * （hi<=h2i,hi<=2i+1）(i=1,2,...,n/2)时称之为堆。在这里只讨论满足前者条件的堆。由堆的
     *
     * 定义可以看出，堆顶元素（即第一个元素）必为最大项（大顶堆）。完全二叉树可以很直观
     *
     * 地表示堆的结构。堆顶为根，其它为左子树、右子树。初始时把要排序的数的序列看作是一
     *
     * 棵顺序存储的二叉树，调整它们的存储序，使之成为一个堆，这时堆的根节点的数最大。然
     *
     * 后将根节点与堆的最后一个节点交换。然后对前面(n-1)个数重新调整使之成为堆。依此类
     *
     * 推，直到只有两个节点的堆，并对它们作交换，最后得到有 n个节点的有序序列。从算法
     *
     * 描述来看，堆排序需要两个过程，一是建立堆，二是堆顶与堆的最后一个元素交换位置。所
     *
     * 以堆排序有两个函数组成。一是建堆的渗透函数，二是反复调用渗透函数实现排序的函数。
     *
     * @param arr
     * @return
     */
    public static int[] heapSort(int[] arr) {
        int arrayLength = arr.length;
        // 循环建堆
        for (int i = 0; i < arrayLength - 1; i++) {
            // 建堆
            buildMaxHeap(arr, arrayLength - 1 - i);
            // 交换堆顶和最后一个元素
            swap(arr, 0, arrayLength - 1 - i);
        }
        return arr;
    }

    private static void swap(int[] data, int i, int j) {
        // TODO Auto-generated method stub
        int tmp = data[i];
        data[i] = data[j];
        data[j] = tmp;
    }

    // 对data 数组从0到lastIndex 建大顶堆

    private static void buildMaxHeap(int[] data, int lastIndex) {
        // TODO Auto-generated method stub
        // 从lastIndex 处节点（最后一个节点）的父节点开始
        for (int i = (lastIndex - 1) / 2; i >= 0; i--) {
            // k 保存正在判断的节点
            int k = i;
            // 如果当前k节点的子节点存在
            while (k * 2 + 1 <= lastIndex) {
                // k 节点的左子节点的索引
                int biggerIndex = 2 * k + 1;
                // 如果biggerIndex 小于lastIndex，即biggerIndex+1 代表的k 节点的右子节点存在
                if (biggerIndex < lastIndex) {
                    // 若果右子节点的值较大
                    if (data[biggerIndex] < data[biggerIndex + 1]) {
                        // biggerIndex 总是记录较大子节点的索引
                        biggerIndex++;
                    }
                }

                // 如果k节点的值小于其较大的子节点的值
                if (data[k] < data[biggerIndex]) {
                    // 交换他们
                    swap(data, k, biggerIndex);
                    // 将biggerIndex 赋予k，开始while 循环的下一次循环，重新保证k节点的值大于其左右子节点的值
                    k = biggerIndex;
                } else {
                    break;
                }
            }
        }
    }

    /**
     * 归并排序
     *
     * 基本排序：归并（Merge）排序法是将两个（或两个以上）有序表合并成一个新的有
     *
     * 序表，即把待排序序列分为若干个子序列，每个子序列是有序的。然后再把有序子序列合并
     *
     * 为整体有序序列。
     *
     * @param arr
     * @return
     */
    public static int[] mergingSort(int arr[]) {
        sort(arr, 0, arr.length - 1);
        return arr;
    }

    private static void sort(int[] data, int left, int right) {
        // TODO Auto-generatedmethod stub
        if (left < right) {
            // 找出中间索引
            int center = (left + right) / 2;
            // 对左边数组进行递归
            sort(data, left, center);
            // 对右边数组进行递归
            sort(data, center + 1, right);
            // 合并
            merge(data, left, center, right);
        }
    }

    private static void merge(int[] data, int left, int center, int right) {
        // TODO Auto-generatedmethod stub
        int[] tmpArr = new int[data.length];
        int mid = center + 1;
        // third 记录中间数组的索引
        int third = left;
        int tmp = left;
        while (left <= center && mid <= right) {
            // 从两个数组中取出最小的放入中间数组
            if (data[left] <= data[mid]) {
                tmpArr[third++] = data[left++];
            } else {
                tmpArr[third++] = data[mid++];
            }
        }
        // 剩余部分依次放入中间数组
        while (mid <= right) {
            tmpArr[third++] = data[mid++];
        }
        while (left <= center) {
            tmpArr[third++] = data[left++];
        }
        // 将中间数组中的内容复制回原数组

        while (tmp <= right) {
            data[tmp] = tmpArr[tmp++];
        }

    }

    /**
     * 基数排序
     *
     * （1）基本思想：将所有待比较数值（正整数）统一为同样的数位长度，数位较短的数前面
     *
     * 补零。然后，从最低位开始，依次进行一次排序。这样从最低位排序一直到最高位排序完成
     *
     * 以后,数列就变成一个有序序列。
     *
     * @param arr
     * @return
     */
    public static int[] radixSort(int arr[]) {
        sort(arr);
        return arr;
    }

    private static void sort(int[] array) {
        // 首先确定排序的趟数;
        int max = array[0];
        for (int i = 1; i < array.length; i++) {
            if (array[i] > max) {
                max = array[i];
            }
        }
        int time = 0;
        // 判断位数;
        while (max > 0) {
            max /= 10;
            time++;
        }
        // 建立10个队列;
        List<ArrayList> queue = new ArrayList<ArrayList>();
        for (int i = 0; i < 10; i++) {
            ArrayList<Integer> queue1 = new ArrayList<Integer>();
            queue.add(queue1);
        }
        // 进行time 次分配和收集;
        for (int i = 0; i < time; i++) {
            // 分配数组元素;
            for (int j = 0; j < array.length; j++) {
                // 得到数字的第time+1 位数;
                int x = array[j] % (int) Math.pow(10, i + 1) / (int) Math.pow(10, i);
                ArrayList<Integer> queue2 = queue.get(x);
                queue2.add(array[j]);
                queue.set(x, queue2);
            }
            int count = 0;// 元素计数器;
            // 收集队列元素;
            for (int k = 0; k < 10; k++) {
                while (queue.get(k).size() > 0) {
                    ArrayList<Integer> queue3 = queue.get(k);
                    array[count] = queue3.get(0);
                    queue3.remove(0);
                    count++;
                }
            }
        }
    }
}