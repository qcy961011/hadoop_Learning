package algorithm;

public class SwapNodesInPairs {

    public static void main(String[] args) {
        ListNode listnode_2 = new ListNode(2);
        ListNode listnode_4 = new ListNode(4);
        ListNode listnode_3 = new ListNode(3);
        ListNode listnode_5 = new ListNode(5);
        listnode_2.next = listnode_4;
        listnode_4.next = listnode_3;
        listnode_3.next = listnode_5;
        ListNode listNode = swapPairs(listnode_2);
        while (listNode != null) {
            System.out.println(listNode.val);
            listNode = listNode.next;
        }
    }

    private static ListNode swapPairs(ListNode head) {
        if (head == null || head.next == null) {
            return head;
        }
        // 1. 得到next节点
        ListNode next = head.next;
        // 2. 将后续节点处理后拼接到第一个节点后
        head.next = swapPairs(next.next);
        // 3. next的下个节点为head（交换处理）
        next.next = head;
        // 4. 返回新head（next节点为新head节点）
        return next;
    }

    private static class ListNode {
        int val;
        ListNode next;

        ListNode(int x) {
            val = x;
        }
    }
}
