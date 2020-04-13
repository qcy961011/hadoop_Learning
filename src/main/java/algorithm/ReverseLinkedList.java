package algorithm;

public class ReverseLinkedList {

    public static void main(String[] args) {
        ReverseLinkedList.ListNode listnode_2 = new ReverseLinkedList.ListNode(2);
        ReverseLinkedList.ListNode listnode_4 = new ReverseLinkedList.ListNode(4);
        ReverseLinkedList.ListNode listnode_3 = new ReverseLinkedList.ListNode(3);
        listnode_2.next = listnode_4;
        listnode_4.next = listnode_3;
        reverse(listnode_2);
    }


    private static ListNode reverse(ListNode listNode) {
        ListNode prev = null;
        ListNode curr = listNode;
        while (curr != null) {
            // 1. 提出链表下一个节点
            ListNode next = curr.next;
            // 2. 把当前节点的下一个节点改为上一个节点
            curr.next = prev;
            // 3. 保存这个节点
            prev = curr;
            // 4. 将下一个执行节点改完next
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
