package com.qiao.test.wordCount.driverTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class FsShellTest {

    Configuration conf = null;
    FsShell shell = null;
    FileSystem fs = null;
    @Before
    public void before() throws IOException {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:8020");
        shell = new FsShell();
        conf.setQuietMode(false);
        shell.setConf(conf);
        fs = FileSystem.get(conf);
        System.out.println("配置加载成功");
    }

    @Test
    public void exists() throws Exception {
        System.out.println(fs.exists(new Path("test/wordCount/output/")));
    }

    @Test
    public void rmSkipTrash() throws Exception {
        String[] args = {"-rm", "-skipTrash", "/user/10508/.Trash/Current/data/test1.txt"};
        int res = -1;
        res = ToolRunner.run(shell, args);
        String msg = res == 0 ? "命令执行成功" : "命令执行失败";
        System.out.println(msg);
        System.exit(res);
    }

    @Test
    public void cp() throws Exception {
        String[] args = {"-cp", "/user/10508/.Trash/Current/data/test1.txt", "/data/"};
        int res = -1;
        res = ToolRunner.run(shell, args);
        String msg = res == 0 ? "命令执行成功" : "命令执行失败";
        System.out.println(msg);
        System.exit(res);
    }

    @Test
    public void lsr() throws Exception {
        String[] args = {"-ls", "-R", "/"};
        int res = -1;
        res = ToolRunner.run(shell, args);
        String msg = res == 0 ? "命令执行成功" : "命令执行失败";
        System.out.println(msg);
        System.exit(res);
    }

    @Test
    public void remove() throws Exception {
        String[] args = {"-rm","-R", "/test/wordCount/output/"};
        int res = -1;
        res = ToolRunner.run(shell, args);
        String msg = res == 0 ? "命令执行成功" : "命令执行失败";
        System.out.println(msg);
        System.exit(res);
    }

    @Test
    public void put() throws Exception {
        String[] args = {"-put", "D:\\data\\test.txt", "/test/wordCount/input/"};
        int res = -1;
        try {
            res = ToolRunner.run(shell, args);
        } finally {
            shell.close();
        }
        String msg = res == 0 ? "命令执行成功" : "命令执行失败";
        System.out.println(msg);
        System.exit(res);
    }

    @Test
    public void mkdir() throws Exception {
        String[] args = {"-mkdir", "/test/wordCount/input/"};
        int res = -1;
        try {
            res = ToolRunner.run(shell, args);
        } finally {
            shell.close();
        }
        String msg = res == 0 ? "命令执行成功" : "命令执行失败";
        System.out.println(msg);
        System.exit(res);
    }


    @Test
    public void ls() throws Exception {
        String[] args = {"-ls", "/data"};
        int res = -1;
        try {
            res = ToolRunner.run(shell, args);
        } finally {
            shell.close();
        }
        String msg = res == 0 ? "命令执行成功" : "命令执行失败";
        System.out.println(msg);
        System.exit(res);
    }

    @After
    public void after() {
        conf = null;
        shell = null;
        fs = null;
        System.out.println("测试结束");
    }

}
