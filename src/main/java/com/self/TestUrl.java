package com.self;


import java.net.MalformedURLException;
import java.net.URL;

public class TestUrl {
    public static void main(String[] args) throws MalformedURLException {
        String url = "http://hainiubl.com/static/xueyuan.html";
        URL test = new URL(url);
        System.out.println(test.getAuthority());
        System.out.println(test.getPath());
        System.out.println(test.getFile());
        System.out.println(test.getHost());
        System.out.println(test.getRef());
        System.out.println(test.getUserInfo());
        System.out.println(test.getProtocol());


    }
}
