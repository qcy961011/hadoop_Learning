package com.self;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

//    public static void main(String[] args) {
//        ItemManager item = new ItemManagerImpl();
//        InvocationHandler invocationHandler = new DynamicProxy(item);
//        ItemManager itemProxy = (ItemManager) Proxy.newProxyInstance(item.getClass().getClassLoader(),
//                item.getClass().getInterfaces(), invocationHandler);
//        itemProxy.addItem("123" , 123);
//        itemProxy.delItem("123" , 123);
//    }

    public static void main(String[] args) {
        String data = "20190701\t12\t123\t1\t爆米花\thttp://47.110.229.195:8080/PVSTAT/test\thttp://www.baidu.com";
        Pattern pattern = Pattern.compile("\t.*?\t");
        Matcher matcher = pattern.matcher(data);
        while (matcher.find()) {
            System.out.println(matcher.group(0));
        }
    }
}
