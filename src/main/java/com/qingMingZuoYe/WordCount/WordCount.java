package com.qingMingZuoYe.WordCount;

import java.io.*;
import java.util.*;

public class WordCount {

    private static Map<String, List<Integer>> mapper = new HashMap<>();
    private static Map<String, Integer> reducer = new HashMap<>();


    public static void main(String[] args) throws IOException {
        String path = "test/qingMingHomeWork/input";
        File[] files = getFiles(path);
        String output = "test/qingMingHomeWork/output.txt";
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(output))));
        for (File file : files) {
            mapper(file);
        }
        reducer();
        Iterator<String> iterator = reducer.keySet().iterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            bufferedWriter.write(next + "\t" + reducer.get(next) + "\n");
            bufferedWriter.flush();
            System.out.println(next + " : " + reducer.get(next));
        }
        bufferedWriter.close();

    }


    public static File[] getFiles(String path) {
        File[] files = null;
        File dir = new File(path);
        files = dir.listFiles();
        return files;
    }

    public static void mapper(File file) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        String line = "";
        while ((line = bufferedReader.readLine()) != null) {
            String[] strArr = line.split(" ");
            for (String str :
                    strArr) {
                if (mapper.get(str) != null) {
                    List list = mapper.get(str);
                    list.add(1);
                } else {
                    List<Integer> list = new ArrayList<>();
                    list.add(1);
                    mapper.put(str, list);
                }
            }
        }
        bufferedReader.close();
    }

    public static void reducer() {
        Iterator<String> iterator = mapper.keySet().iterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            int count = mapper.get(next).size();
            reducer.put(next, count);
        }
    }


}
