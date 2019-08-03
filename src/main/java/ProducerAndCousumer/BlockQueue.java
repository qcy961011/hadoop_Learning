package ProducerAndCousumer;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class BlockQueue {

    private static final int SIZE = 5;


    public static void main(String[] args) {

        BlockingQueue<Integer> queue = new LinkedBlockingDeque<>();

        Producer p1 = new Producer(queue , "一生" ,SIZE);
        Producer p2 = new Producer(queue , "二生" ,SIZE);
        Consumer consumer = new Consumer(queue, "一消", SIZE);

        p1.start();
        p2.start();
        consumer.start();
    }


    public static class Producer extends Thread {

        private BlockingQueue<Integer> queue;

        String name;

        int maxSize;

        int i = 0;

        public Producer(BlockingQueue<Integer> queue, String name, int maxSize) {
            super(name);
            this.queue = queue;
            this.name = name;
            this.maxSize = maxSize;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    System.out.println("[ " + name + " ] Producing value : " + i);
                    queue.put(i++);

                    Thread.sleep(new Random().nextInt(1000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }
        }
    }

    public static class Consumer extends Thread {
        private BlockingQueue<Integer> queue;

        String name;

        int maxSize;

        public Consumer(BlockingQueue<Integer> queue, String name, int maxSize) {
            super(name);
            this.queue = queue;
            this.name = name;
            this.maxSize = maxSize;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Integer take = queue.take();
                    System.out.println("[ " + name + " ] Consuming : " + take);
                    Thread.sleep(new Random().nextInt(1000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}
