package com.qiao.test.maxWordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxWordCountMapper extends Mapper<LongWritable , Text , Text , IntWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strArr = value.toString().split("\t");
        if(strArr.length != 2) {
            context.getCounter("user" , "error").increment(1);
            return;
        }
        context.write(new Text(strArr[0]) , new IntWritable(Integer.parseInt(strArr[1])));
    }
}
