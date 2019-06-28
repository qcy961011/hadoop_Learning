package com.results.sex;

import com.qiao.test.baseMr.BaseMR;
import com.results.People;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SexDiver extends BaseMR {

    private static class SexMapper extends Mapper<LongWritable , Text , People , Text> {

        People people = new People();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = value.toString().split(";");
            if (strArr.length != 4) {
                context.getCounter("user" , "error").increment(1L);
                return;
            }
            people.setName(new Text(strArr[1]));
            people.setAge(new IntWritable(Integer.parseInt(strArr[3])));
            people.setSex(new Text(strArr[2]));
            context.write(people , new Text(strArr[2]));
        }
    }

    private static class SexReducer extends Reducer<People , Text , Text , Text> {
        @Override
        protected void reduce(People key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value:
                 values) {
                context.write(value , new Text(key.getAge().toString()));
            }
        }
    }

    private static class SexCompar extends WritableComparator {
        public SexCompar(){
            super(People.class , true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            People ap = (People) a;
            People bp = (People) b;
            if("boy".equals(ap.getSex().toString())){
                return 1;
            } else {
                return -1;
            }
        }
    }

    private static class SexPartition extends Partitioner<People , Text> {
        @Override
        public int getPartition(People people, Text text, int numPartitions) {
            if(12 == people.getAge().get()) {
                return 1;
            } else {
                return 0;
            }
        }
    }



    @Override
    public Job getJob() throws IOException {

        Job job = Job.getInstance(getConfiguration() , getJobNameWithTaskId());

        job.setJarByClass(SexDiver.class);
        job.setMapperClass(SexMapper.class);
        job.setReducerClass(SexReducer.class);
        job.setPartitionerClass(SexPartition.class);
        job.setSortComparatorClass(SexCompar.class);

        job.setNumReduceTasks(2);
        job.setMapOutputKeyClass(People.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job , getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job , getOutputPath(getJobNameWithTaskId()));

        return job;
    }

    @Override
    public String getJobName() {
        return "SexDiver";
    }
}
