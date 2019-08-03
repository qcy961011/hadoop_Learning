package ProducerAndCousumer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockQueue {
    private static final int SIZE = 5;

    private static final Lock lock = new ReentrantLock();

    /**
     * 队列已满条件
     */
    private static final Condition fullCondition = lock.newCondition();

    /**
     * 队列为空条件
     */
    private static final Condition emptyCondition = lock.newCondition();

    public static void main(String[] args) {
        Queue<Integer> queue = new LinkedList<Integer>();

        Producer p1 = new Producer(queue, "一生", SIZE);
        Consumer c1 = new Consumer(queue, "一消", SIZE);

        p1.start();
        c1.start();
    }



    public static class Producer extends Thread {

        private Queue<Integer> queue;

        String name;

        int maxSize;

        int i = 0;


        public Producer(Queue<Integer> queue, String name, int maxSize) {
            super(name);
            this.queue = queue;
            this.name = name;
            this.maxSize = maxSize;
        }

        @Override
        public void run() {
            while (true) {
                lock.lock();
                while (queue.size() == maxSize) {
                    try {
                        System.out.println("Queue is full , Producer [ " + name + " ]");
                        fullCondition.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("[ " + name + " ] producing value : " + i);
                queue.offer(i++);

                // 唤醒其他所有生成者 、 消费者
                fullCondition.signalAll();
                emptyCondition.signalAll();

                // 释放锁
                lock.unlock();

                try {
                    Thread.sleep(new Random().nextInt(1000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    public static class Consumer extends Thread {
        private Queue<Integer> queue;

        String name;

        int maxSize;

        public Consumer(Queue<Integer> queue, String name, int maxSize) {
            this.queue = queue;
            this.name = name;
            this.maxSize = maxSize;
        }

        @Override
        public void run() {
            while (true) {
                lock.lock();
                while (queue.isEmpty()){
                    try {
                        System.out.println("Queue is empty , Consumer [ " + name + " ]");
                        emptyCondition.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                Integer poll = queue.poll();
                System.out.println("[" + name + "] Consuming value : " + poll);

                fullCondition.signalAll();
                emptyCondition.signalAll();

                lock.unlock();

                try {
                    Thread.sleep(new Random().nextInt(1000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    }


}
