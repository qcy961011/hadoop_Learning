package com.qiao.test.teacherHomeWork.max;

import com.qiao.test.baseMr.BaseMR;
import com.qiao.test.teacherHomeWork.sumReslut.SumReslut;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxReslut extends BaseMR {

    private static class MaxReslutMapper extends Mapper<LongWritable , Text, IntWritable , Text> {
//        private StudentWritable studentWritable = new StudentWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = value.toString().split("\t");
            if (strArr.length != 2) {
                context.getCounter("user" , "error").increment(1);
                return;
            }
            StringBuffer buffer = new StringBuffer();
            buffer.append(strArr[0] + " == " + strArr[1]);
            context.write(new IntWritable(Integer.parseInt(strArr[1])), new Text(buffer.toString()));
        }
    }

    private static class MaxReslutReducer extends Reducer<IntWritable , Text , Text , IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value:
                 values) {
                context.write(new Text(value.toString().split(" == ")[0]) , new IntWritable(Integer.parseInt(value.toString().split(" == ")[1])));
            }
        }
    }


    private static class ReslutcComper extends WritableComparator {
        public ReslutcComper() {
            super(IntWritable.class , true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }


    @Override
    public Job getJob() throws IOException {
        Job job = Job.getInstance(getConfiguration());

        job.setJarByClass(MaxReslut.class);
        job.setMapperClass(MaxReslutMapper.class);
        job.setReducerClass(MaxReslutReducer.class);
        job.setSortComparatorClass(ReslutcComper.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        SumReslut sumReslut = new SumReslut();

        FileInputFormat.addInputPath(job , sumReslut.getOutputPath(sumReslut.getJobNameWithTaskId()));
        FileOutputFormat.setOutputPath(job , getOutputPath(getJobNameWithTaskId()));
        return job;
    }

    @Override
    public String getJobName() {
        return "MaxReslut";
    }

}
