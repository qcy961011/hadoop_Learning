package com.qiao.classLoder;

import org.junit.Test;

public class ClassLoderTest {

    @Test
    public void classLoderTest() {
        System.out.println(ClassLoader.getSystemClassLoader().getClass().getName());
        System.out.println(ClassLoader.getSystemClassLoader().getParent());
        System.out.println(ClassLoader.getSystemClassLoader().getParent().getParent());
    }
}
