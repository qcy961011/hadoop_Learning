package com.qiao.test.test;

import java.util.HashSet;
import java.util.Set;

/**
 * This class consists exclusively of static methods that operate on or return
 * collections.  It contains polymorphic algorithms that operate on
 * collections, "wrappers", which return a new collection backed by a
 * specified collection, and a few other odds and ends.
 *
 * @author 乔纯宇
 * @see this is a Morse
 */
public class MorseRepresentations {
    private static String[] dir = {".-", "-...", "-.-.", "-..", ".", "..-.", "--.", "....", "..", ".---", "-.-", ".-..", "--", "-.", "---", ".--.", "--.-", ".-.", "...", "-", "..-", "...-", ".--", "-..-", "-.--", "--.."};

    private static String letter = "abcdefghijklmnopqrstuvwxyz";

    public static int uniqueMorseRepresentations(String[] words) {
        Set<String> result = new HashSet<>();
        for (String str : words) {
            StringBuffer sb = new StringBuffer();
            for (char c :
                    str.toCharArray()) {
                sb.append(dir[c - 'a']);
            }
            if (!result.contains(sb.toString())) {
                result.add(sb.toString());
            }
        }
        return result.size();
    }

    public static void main(String[] args) {
        String[] words = {"gin", "zen", "gig", "msg"};
        System.out.println(uniqueMorseRepresentations(words));
    }
}
