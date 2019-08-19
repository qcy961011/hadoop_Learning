package algorithm;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class ValidParentheses {

    public static void main(String[] args) {
        String test = "([)]";
        System.out.println(isValidString(test));
    }

    /**
     * 使用栈
     * 左压栈
     * 右判断栈顶，正确则出栈
     * 栈最后为空则正确
     */
    private static boolean isValid(String s) {
        Map<Character, Character> mappings = new HashMap<Character, Character>();
        mappings.put(')', '(');
        mappings.put('}', '{');
        mappings.put(']', '[');

        Stack<Character> stack = new Stack<>();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (mappings.containsKey(c)) {
                char o = stack.empty() ? '#' : stack.pop();
                if (mappings.get(c) != o) {
                    return false;
                }
            } else {
                stack.push(c);
            }
        }
        return stack.empty();
    }

    /**
     * 每次替换一对字符串
     * 如果最后字符串长度不为0
     * 则不是合法字符串
     */
    private static boolean isValidString(String s) {
        int tempLength = 0;
        do {
            tempLength = s.length();
            s = s.replace("()" , "").replace("{}" , "").replace("[]","");
        } while (s.length() != tempLength);
        return s.length() == 0;
    }



}
