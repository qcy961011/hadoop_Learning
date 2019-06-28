package other;

public class Test1 {

    public static void def(String str){
        str = "welcome";
    }

    public static void main(String[] args) {
        String str = "123";
        def(str);
        System.out.println(str);
    }
}
