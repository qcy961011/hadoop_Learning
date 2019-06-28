package com.qingMingZuoYe.slefStack;

public class selfStack {

    private Object[] firstArr = new Object[10];

    private int firstIndex = 0;

    private Object[] lastArr = new Object[10];

    private int laseIndex = 0;

    private int popIndex = 0;

    public void push(Object e) {
        if (firstIndex == 0 && laseIndex != 0) {
            for (int i = laseIndex - 1; i >= popIndex; i--) {
                firstArr[firstIndex] = lastArr[i];
                firstIndex++;
            }
            laseIndex = 0;
            lastArr = new Object[10];
        }
        firstArr[firstIndex] = e;
        firstIndex++;
    }

    public Object pop() {
        if (firstIndex != 0) {
            for (int i = firstIndex - 1; i >= 0; i--) {
                lastArr[laseIndex] = firstArr[i];
                laseIndex++;
            }
            firstIndex = 0;
            firstArr = new Object[10];
            popIndex = 0;
        } else if(firstIndex == 0 && laseIndex == 0) {
            String ex = "pop异常，请确认元素数量";
            System.out.println(ex);
            return null;
        }
        Object o = lastArr[popIndex];
        popIndex++;
        return o;
    }

    public static void main(String[] args) {
        selfStack selfStack = new selfStack();

        selfStack.push("1");
        selfStack.push("2");
        selfStack.push("3");
        selfStack.push("4");
        selfStack.push("5");
        selfStack.push("6");
        selfStack.push("7");
        selfStack.push("8");
        selfStack.push("9");
        selfStack.push("10");
        for (int i = 0; i < 9; i++) {
            System.out.println(selfStack.pop());
        }
        selfStack.push("11");
        selfStack.push("12");
        selfStack.push("13");
        selfStack.push("14");
        selfStack.push("15");
        for (int i = 0; i < 3; i++) {
            System.out.println(selfStack.pop());
        }


    }
}
