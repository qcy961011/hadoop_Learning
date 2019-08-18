package algorithm;

import java.util.HashSet;
import java.util.Set;

public class LinkedListCycle {

    public static void main(String[] args) {
        ListNode listnode_2 = new ListNode(2);
        ListNode listnode_4 = new ListNode(4);
        ListNode listnode_3 = new ListNode(3);
        ListNode listnode_5 = new ListNode(5);
        listnode_2.next = listnode_4;
        listnode_4.next = listnode_3;
        listnode_3.next = listnode_5;
        listnode_5.next = listnode_4;

        boolean flag = hasCycleSet(listnode_2);
        System.out.println(flag);
    }


    /**
     * 利用Set集合无序特性，判断是否有重复元素
     */
    private static boolean hasCycleSet(ListNode head) {
        Set set = new HashSet();
        while (head != null) {
            if (set.contains(head)){
                return true;
            } else {
                set.add(head);
            }
            head = head.next;
        }
        return false;
    }


    /**
     * 快慢指针式
     */
    private static boolean hasCycle(ListNode head) {
        if (head == null || head.next == null) {
            return false;
        }
        ListNode slow = head;
        ListNode fast = head.next;
        while (slow != fast){
            if (fast == null || fast.next == null){
                return false;
            }
            slow = slow.next;
            fast = fast.next.next;
        }
        return true;
    }

    private static class ListNode {
        int val;
        ListNode next;

        ListNode(int x) {
            val = x;
        }
    }
}
