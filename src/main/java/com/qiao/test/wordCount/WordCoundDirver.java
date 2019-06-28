package com.qiao.test.wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCoundDirver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS" , args[0]);

        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCoundDirver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountRudece.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job , new Path("/test/wordCount/input/"));
        FileOutputFormat.setOutputPath(job , new Path("/test/wordCount/output/"));

        boolean result = job.waitForCompletion(true);
        String msg = result ? "命令执行成功" : "命令执行失败" ;
        System.out.println(msg);
        System.exit(result ? 0 : -1);
    }

}
