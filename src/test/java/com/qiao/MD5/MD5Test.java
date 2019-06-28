package com.qiao.MD5;

import org.junit.Test;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Test {

    @Test
    public void stringToMD5() throws NoSuchAlgorithmException {
        String test = "PRJNA316730";
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(test.getBytes());
        System.out.println(new BigInteger(1, md.digest()).toString(16));
    }
}
