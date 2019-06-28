package com.qiao.test.teacherHomeWork.sumReslut;

import com.qiao.test.baseMr.BaseMR;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SumReslut extends BaseMR {

    private static class SumReslutMapper extends Mapper<LongWritable , Text , Text , IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] arrStr = value.toString().split(";");
            if(arrStr.length != 2){
                context.getCounter("user" , "error").increment(1);
                return;
            }
            context.write(new Text(arrStr[0]) , new IntWritable(Integer.parseInt(arrStr[1])));

        }
    }

    private static class SumReslutReducer extends Reducer<Text , IntWritable , Text , IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value:
                 values) {
                sum += value.get();
            }
            context.write(key , new IntWritable(sum));
        }
    }
    @Override
    public Job getJob() throws IOException {
        Job job = Job.getInstance(getConfiguration() , getJobNameWithTaskId());

        job.setJarByClass(SumReslut.class);

        job.setMapperClass(SumReslutMapper.class);
        job.setReducerClass(SumReslutReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job , getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job , getOutputPath(getJobNameWithTaskId()));

        return job;
    }

    @Override
    public String getJobName() {
        return "SumReslut";
    }
}
