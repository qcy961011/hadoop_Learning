package com.qiao.test.distinct;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistinctReduce extends Reducer<Text, NullWritable, NullWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.getCounter("Sensitive Words:", key.toString());
        context.write(NullWritable.get(), key);
    }
}
