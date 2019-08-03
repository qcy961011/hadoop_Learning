package com.qiao.test.wordCount;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class RandomTestFile {

    public static void main(String[] args) throws IOException {
        String[] lib = {"Java ",
                "Hadoop ",
                "Spark ",
                "Hive ",
                "Hbase ",
                "Oozie ",
                "ZooKeeper ",
                "SpringMVC ",
                "Hello ",
                "Big ",
                "Spring ",
                "Flume ",
                "Kafaka ",
                "Redis ",
                "Python ",
                "Scala "};
        Random random = new Random();
        StringBuffer stringBuffer = new StringBuffer();
        FileOutputStream fileOutputStream = new FileOutputStream(new File("D://data//data1.txt"));
        int count = 0 ;
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                for (int j = 0; j < 6000000; j++) {
                    stringBuffer.append(lib[random.nextInt(9)]);
                    if (j % 100 == 0) {
                        stringBuffer.append("\n");
                        try {
                            fileOutputStream.write(stringBuffer.toString().getBytes());
                            stringBuffer.setLength(0);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            thread.start();
        }
    }
}
