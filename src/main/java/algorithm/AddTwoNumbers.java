package algorithm;

public class AddTwoNumbers {

    public static void main(String[] args) {
        ListNode listnode_2 = new ListNode(2);
        ListNode listnode_4 = new ListNode(4);
        ListNode listnode_3 = new ListNode(3);
        ListNode listnode_5 = new ListNode(5);
        ListNode listnode_6 = new ListNode(6);
        ListNode listnode_4_2 = new ListNode(4);
        listnode_2.next = listnode_4;
        listnode_4.next = listnode_3;
        listnode_5.next = listnode_6;
        listnode_6.next = listnode_4_2;
        addTwoNumbers(listnode_2 , listnode_5);
    }

    public static ListNode addTwoNumbers(ListNode l1, ListNode l2) {
        ListNode preNode = new ListNode(0);
        ListNode curr = preNode;
        int carry = 0;
        while (l1 != null || l2 != null){
            int v_1 = l1 != null ? l1.val : 0;
            int v_2 = l2 != null ? l2.val : 0;
            int sum = carry + v_1 + v_2;
            carry = sum / 10;
            curr.next = new ListNode(sum % 10);
            curr = curr.next;
            l1 = l1 != null ? l1.next : null;
            l2 = l2 != null ? l2.next : null;
        }
        if (carry > 0){
            curr.next = new ListNode(carry);
        }
        return preNode.next;
    }

}

class ListNode {
    int val;
    ListNode next;

    ListNode(int x) {
        val = x;
    }
}