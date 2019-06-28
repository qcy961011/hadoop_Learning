package com.qiao.test.wordCount.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WDMapper extends Mapper<LongWritable , Text , Text , IntWritable> {

    private IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArr = value.toString().split("\t");
        for (String str:
             strArr) {
            context.write(new Text(str) , ONE);
        }

    }
}
