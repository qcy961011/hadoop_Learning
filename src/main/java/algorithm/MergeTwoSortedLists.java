package algorithm;

import java.util.List;

public class MergeTwoSortedLists {

    public static void main(String[] args) {
        ListNode listnode_1_1 = new ListNode(1);
        ListNode listnode_1_2 = new ListNode(2);
        ListNode listnode_1_4 = new ListNode(4);
        ListNode listnode_2_1 = new ListNode(1);
        ListNode listnode_2_3 = new ListNode(3);
        ListNode listnode_2_4 = new ListNode(4);
        listnode_1_1.next = listnode_1_2;
        listnode_1_2.next = listnode_1_4;
        listnode_2_1.next = listnode_2_3;
        listnode_2_3.next = listnode_2_4;
        ListNode listNode = mergeTwoLists(listnode_1_1, listnode_2_1);
        while (listNode != null) {
            System.out.println(listNode.val);
            listNode = listNode.next;
        }
    }


    /**
     * 使用递归
     * 每次返回最小值
     * 每次将节点拼接为链
     */
    private static ListNode mergeTwoLists(ListNode l1, ListNode l2) {
        if (l1 == null) {
            return l2;
        } else if (l2 == null) {
            return l1;
        }else if (l1.val < l2.val) {
            l1.next = mergeTwoLists(l1.next , l2);
            return l1;
        } else {
            l2.next = mergeTwoLists(l1 , l2.next);
            return l2;
        }
    }

    /**
     * 循环每个链表
     * 将小值拼接在临时头节点之后
     * 返回头节点的next
     */
    private static ListNode mergeTwoListsTwo(ListNode l1, ListNode l2) {
        ListNode prevHead = new ListNode(-1);
        ListNode prev = prevHead;
        while (l1 != null && l2 != null) {
            if (l1.val < l2.val){
                prev.next = l1;
                l1 = l1.next;
            } else {
                prev.next = l2;
                l2 = l2.next;
            }
            prev = prev.next;
        }
        prev.next = l1 == null ? l2 : l1;
        return prevHead.next;
    }



    private static class ListNode {
        int val;
        ListNode next;
        ListNode(int x) {
            val = x;
        }
    }
}
