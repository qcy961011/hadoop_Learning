package algorithm;

import java.util.Stack;

/**
 * 使用栈实现队列
 * FILO -> FIFO
 */
public class ImplementStackUsingQueues {

    public static void main(String[] args) {
        MyStack myStack = new MyStack();

        myStack.push(1);
        myStack.push(2);
        System.out.println(myStack.top());
        System.out.println(myStack.pop());
        System.out.println(myStack.empty());
    }


    private static class MyStack {
        Stack<Integer> inputStack;
        Stack<Integer> outputStack;

        /** Initialize your data structure here. */
        public MyStack() {
            inputStack = new Stack<>();
            outputStack = new Stack<>();
        }

        /** Push element x onto stack. */
        public void push(int x) {
            if (outputStack.empty()) {
                while (!inputStack.empty()) {
                    outputStack.push(inputStack.pop());
                }
            }
            inputStack.push(x);
        }

        /** Removes the element on top of the stack and returns that element. */
        public int pop() {
            if (outputStack.empty()) {
                while (!inputStack.empty()) {
                    outputStack.push(inputStack.pop());
                }
            }
            return outputStack.pop();
        }

        /** Get the top element. */
        public int top() {
            return outputStack.peek();
        }

        /** Returns whether the stack is empty. */
        public boolean empty() {
            return inputStack.empty() && outputStack.empty();
        }
    }
}


