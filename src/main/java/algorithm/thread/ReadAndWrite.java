package algorithm.thread;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ReadAndWrite {
    public static void main(String[] args) {
        final FileRW f = new FileRW();

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

class FileRW {
    private final Lock lock = new ReentrantLock();
    private int readCount = 0;
    private int writeCount = 0;
    private int waitRead = 0;
    private int waitWrite = 0;
    private final Condition okToRead = lock.newCondition();
    private final Condition okToWrite = lock.newCondition();

    public void read() {
        enterRead();
        System.out.println(Thread.currentThread().getName()
                + " is Reading."
                + " Reader: " + readCount
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
        enterWrite();
        System.out.println(Thread.currentThread().getName() + " is Writing");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        leaveWrite();
    }


    private void enterWrite() {
        lock.lock();
        try {
            while ((readCount + writeCount) > 0) {
                waitWrite++;
                try {
                    okToWrite.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                waitWrite--;
            }
            writeCount++;
        } finally {
            lock.unlock();
        }
    }

    private void leaveWrite() {
        lock.lock();
        try {
            writeCount--;
            if (waitWrite > 0) {
                okToWrite.signalAll();
            } else if (waitRead > 0) {
                okToRead.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }


    private void enterRead() {
        lock.lock();
        try {
            while ((writeCount + waitWrite) > 0) {
                waitRead++;
                try {
                    okToRead.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                waitRead--;
            }
            readCount++;
        } finally {
            lock.unlock();
        }
    }

    private void leaveRead() {
        lock.lock();
        try {
            readCount--;
            if (readCount == 0 && waitWrite > 0) {
                okToWrite.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

}
