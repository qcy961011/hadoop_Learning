package ProducerAndCousumer;

import org.jetbrains.annotations.NotNull;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

public class ObjectQueue {

    private static final int SIZE = 5;

    public static void main(String[] args) {
        Queue<Integer> queue = new LinkedList<Integer>();

        Thread producer1 = new Producer("P-1", queue, SIZE);
        Thread producer2 = new Producer("P-2", queue, SIZE);
        Thread consumer1 = new Consumer("C1", queue, SIZE);
        Thread consumer2 = new Consumer("C2", queue, SIZE);
        Thread consumer3 = new Consumer("C3", queue, SIZE);

        producer1.start();
        producer2.start();
        consumer1.start();
        consumer2.start();
        consumer3.start();
    }

    /**
     * 生产者
     */
    public static class Producer extends Thread {

        private Queue<Integer> queue;

        String name;

        int maxSize;

        int i = 0;

        public Producer(String name, Queue<Integer> queue, int maxSize) {
            super(name);
            this.queue = queue;
            this.name = name;
            this.maxSize = maxSize;
        }

        @Override
        public void run() {
            while (true) {
                synchronized (queue) {
                    while (maxSize == queue.size()) {
                        try {
                            System.out.println("Queue is full , Producer [ " + name + " ]");
                            queue.wait();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("[ " + name + " ] producing value : + " + i);
//                    queue.offer(i++);
                    queue.offer(i++);
                    queue.notifyAll();

                    try {
                        Thread.sleep(new Random().nextInt(1000));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * 消费者
     */
    private static class Consumer extends Thread {
        private Queue<Integer> queue;
        String name;
        int maxSize;

        public Consumer(String name, Queue<Integer> queue, int maxSize) {
            super(name);
            this.queue = queue;
            this.name = name;
            this.maxSize = maxSize;
        }

        @Override
        public void run() {
            while (true) {
                synchronized (queue) {
                    while (queue.isEmpty()){
                        try {
                            System.out.println("Queue is empty , Consumer ["+ name + " ]" );
                            queue.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    int x = queue.poll();
                    System.out.println("[ " + name + " ] Consuming value " + x);
                    queue.notifyAll();
                    try {
                        Thread.sleep(new Random().nextInt(1000));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }


}
