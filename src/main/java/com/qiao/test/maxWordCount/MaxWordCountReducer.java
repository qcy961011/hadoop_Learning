package com.qiao.test.maxWordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxWordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Text outkey = new Text();
    private IntWritable outValue = new IntWritable(Integer.MIN_VALUE);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value:
             values) {
            if(outValue.get() < value.get()){
                outValue.set(value.get());
                outkey.set(key.toString());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(outkey , outValue);
    }
}
