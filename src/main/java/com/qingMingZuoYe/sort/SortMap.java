package com.qingMingZuoYe.sort;

import java.io.*;
import java.util.*;

public class SortMap {


    private static Map<String, List<String>> mapper = new HashMap<>();
    private static Set<String> reducer = new TreeSet<>(new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            Double oo1 = Double.parseDouble(o1.split("==")[0]);
            Double oo2 = Double.parseDouble(o2.split("==")[0]);
            if (oo2.equals(oo1)) {
                return 1;
            }
            return oo1.compareTo(oo2);
        }
    });

    public static void main(String[] args) throws IOException {
        String path = "test/qingMingHomeWork/homeWorkTwo";
        File[] files = getFiles(path);
        String output = "test/qingMingHomeWork/homeWorkTwo/output.txt";
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(output))));
        for (File file : files) {
            mapper(file);
        }
        reducer();
        Iterator<String> iterator = reducer.iterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            bufferedWriter.write(next.split("==")[1] + "\t" + next.split("==")[0] + "\n");
            bufferedWriter.flush();
            System.out.println(next);
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
            String[] strArr = line.split("\t");
            if (mapper.get(strArr[0]) != null) {
                List list = mapper.get(strArr[0]);
                list.add(strArr[2]);
            } else {
                List<String> list = new ArrayList();
                list.add(strArr[2]);
                mapper.put(strArr[0], list);
            }
        }
        bufferedReader.close();
    }


    public static void reducer() {
        Iterator<String> iterator = mapper.keySet().iterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            int count = mapper.get(next).size();
            int sum = 0;
            for (String str :
                    mapper.get(next)) {
                sum += Integer.parseInt(str);
            }
            reducer.add(sum / count * 1.0 + "==" + next);
        }
    }

}
