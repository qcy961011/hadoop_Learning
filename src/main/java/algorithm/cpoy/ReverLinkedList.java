package algorithm.cpoy;



public class ReverLinkedList {

    public static void main(String[] args) {
        ListNode listnode_2 = new ListNode(2);
        ListNode listnode_4 = new ListNode(4);
        ListNode listnode_3 = new ListNode(3);
        listnode_2.next = listnode_4;
        listnode_4.next = listnode_3;
        reverse(listnode_2);
    }

    private static ListNode reverse(ListNode listNode) {
        // 当前节点
        ListNode curr = listNode;
        // 前驱节点
        ListNode prev = null;
        while (curr.next != null) {
            // 获取下一个节点
            ListNode next = curr.next;
            // 使当前节点的next节点拼接为已反转节点
            curr.next = prev;
            // 当前节点保存为已反转节点
            prev = curr;
            // 头结点后移
            curr = next;
        }
        return prev;
    }

    static class ListNode {
        private int val;
        private ListNode next;

        ListNode(int x) {
            val = x;
        }
    }
}