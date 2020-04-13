package algorithm.thread;

import java.util.concurrent.locks.*;

public class ProducerAndConsumer {
    public static void main(final String[] args) {
        final fileRW f = new fileRW();

        for (int i = 0; i < 10; i++) {
            new Thread(
                    () -> f.read()
            ).start();
        }
        for (int i = 0; i < 4; i++) {
            new Thread(
                    () -> f.write()
            ).start();
        }
        for (int i = 0; i < 10; i++) {
            new Thread(
                    () -> f.read()
            ).start();
        }

    }
}

class fileRW {
    // 写优先 需要两个条件变量并且需要等待写 主要是增加的条件变量是为了写写分离
    private final Lock lock = new ReentrantLock();
    private int readCnt = 0, writeCnt = 0, waitRead = 0, waitWrite = 0;
    private final Condition okToRead = lock.newCondition();
    private final Condition okToWrite = lock.newCondition();

    public void read() {
        enterRead();
        System.out.println(Thread.currentThread().getName()
                + " is Reading."
                + " Reader: " + readCnt
                + " WaitingReader:" + waitRead);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName() + " read done");
        leaveRead();
    }

    public void write() {
        enterWrite(); // 如果有正在读或正在写 等
        System.out.println(Thread.currentThread().getName() + " is Writing");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        leaveWrite(); // 如果有等待写 唤醒, 如果没有唤醒等待读 写者优先的体现
    }

    private void enterWrite() {
        lock.lock();
        try {
            // 读写互斥, 写写互斥 如果有正在读或正在写
            while (readCnt + writeCnt > 0) {
                waitWrite++;
                try {
                    okToWrite.await();
                } catch (InterruptedException e) {
                }
                waitWrite--;
            }
            writeCnt++;
        } finally {
            lock.unlock();
        }
    }

    private void leaveWrite() {
        lock.lock();
        try {
            writeCnt--;
            // 如果有等待写 唤醒, 如果没有唤醒等待读 写者优先的体现
            if (waitWrite > 0) {
                okToWrite.signalAll();
            }
            else if (waitRead > 0) {
                okToRead.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    private void enterRead() {
        lock.lock();
        try {
            // 读写互斥 一旦有等待写 则阻塞
            while (writeCnt + waitWrite > 0) {
                waitRead++;
                try {
                    okToRead.await();
                } catch (InterruptedException e) {
                }
                waitRead--;
            }
            readCnt++;
        } finally {
            lock.unlock();
        }
    }

    private void leaveRead() {
        lock.lock();
        try {
            readCnt--;
            if (readCnt == 0 && waitWrite > 0) { // 读写互斥 如果没有正在读了 唤醒等待写
                okToWrite.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }
}