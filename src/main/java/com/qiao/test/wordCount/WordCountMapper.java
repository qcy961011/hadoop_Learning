package com.qiao.test.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable , Text , Text , IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("Sensitive Words:","hello");
        String[] strArr = value.toString().split(" ");
        for (String str: strArr) {
            if("hello".equals(str)){
                counter.increment(1L);
            }
            context.write(new Text(str) , new IntWritable(1));
        }

    }
}
