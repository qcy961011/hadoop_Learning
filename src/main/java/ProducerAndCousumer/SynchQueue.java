package ProducerAndCousumer;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

public class SynchQueue {

    private final int MAXSIZE = 5;

    private final int MIXSIZE = 0;

    private Queue<Integer> queue = new LinkedList<>();

    public static void main(String[] args) {
        SynchQueue synchQueue = new SynchQueue();

        Producer producer = synchQueue.new Producer();
        Consumer consumer = synchQueue.new Consumer();

        producer.start();
        consumer.start();

    }

    public class Producer extends Thread {
        @Override
        public void run() {
            producer();
        }

        public synchronized void producer() {
            while (true) {
                synchronized (queue) {
                    while (queue.size() >= MAXSIZE) {
                        try {
                            System.out.println("producer to Max");
                            queue.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    queue.offer(1);
                    queue.notifyAll();
                    System.out.println("add entity");
                    try {
                        Thread.sleep(new Random().nextInt(1000));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public class Consumer extends Thread {
        @Override
        public void run() {
            consumer();
        }

        public synchronized void consumer() {
            while (true) {
                synchronized (queue) {
                    while (queue.size() == MIXSIZE) {
                        try {
                            System.out.println("consumer to Min");
                            queue.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    queue.poll();
                    queue.notifyAll();
                    System.out.println("poll entity");
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
