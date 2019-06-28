package com.qiao.test.wordCount;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class RandomTestFileNumber {

    public static void main(String[] args) throws IOException {
        String[] lib = {"1 ",
                "2 ",
                "3 ",
                "4 ",
                "5 ",
                "6 ",
                "7 ",
                "8 ",
                "9 ",
                "0 "};
        Random random = new Random();
        StringBuffer stringBuffer = new StringBuffer();
        FileOutputStream fileOutputStream = new FileOutputStream(new File("D://data//dataNumber.txt"));
        int count = 0 ;
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                for (int j = 0; j < 100000000; j++) {
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
