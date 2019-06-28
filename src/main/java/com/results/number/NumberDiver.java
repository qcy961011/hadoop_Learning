package com.results.number;

import com.qiao.test.baseMr.BaseMR;
import com.results.sex.SexDiver;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NumberDiver extends BaseMR {

    private static class NumberMapper extends Mapper<LongWritable , Text , Text , LongWritable> {

        private LongWritable ONE = new LongWritable(1L);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = value.toString().split("\t");
            if (strArr.length != 2) {
                context.getCounter("user","error").increment(1);
                return;
            }
            context.write(new Text(strArr[0]) , ONE);
        }
    }

    private static class NumberReduce extends Reducer<Text , LongWritable , Text , LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value:
                 values) {
                count++;
            }
            context.write(key , new LongWritable(count));
        }
    }


    @Override
    public Job getJob() throws IOException {
        Job job = Job.getInstance(getConfiguration() , getJobNameWithTaskId());

        job.setJarByClass(NumberDiver.class);
        job.setMapperClass(NumberMapper.class);
        job.setReducerClass(NumberReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        SexDiver sex = new SexDiver();
        FileInputFormat.addInputPath(job , sex.getOutputPath(sex.getJobNameWithTaskId()));
        FileOutputFormat.setOutputPath(job , getOutputPath(getJobNameWithTaskId()));

        return job;
    }

    @Override
    public String getJobName() {
        return "Nubmer";
    }
}
