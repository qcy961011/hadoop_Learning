package other;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class WordCount {

    private Map<String , Integer> map = new HashMap<>();

    private void map(String str){
        String[] strArr = str.split(" ");
        for (String s:
             strArr) {
            if(map.containsKey(s)){
                map.put(s , map.get(s)+1);
            } else {
                map.put(s , 1);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        WordCount w = new WordCount();
        File f = new File("D:\\data\\data1.txt");
        BufferedReader br = new BufferedReader(new FileReader(f));
        String str = "";
        while ((str = br.readLine()) != null) {
            w.map(str);
        }
        System.out.println(w.map);
    }
}
