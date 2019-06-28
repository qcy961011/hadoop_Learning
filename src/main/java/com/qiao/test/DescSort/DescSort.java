package com.qiao.test.DescSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DescSort {

    private static class DescMapper extends Mapper<LongWritable , Text , LongWritable , Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = value.toString().split(" ");
            if (strArr.length != 2 ){
                context.getCounter("user" , "error").increment(1);
                return;
            }
            context.write(new LongWritable(Long.parseLong(strArr[1])) , new Text(strArr[0]));
        }
    }

    private static class DescReduce extends Reducer<LongWritable , Text , LongWritable , Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value:
                    values) {
                context.write(key , value);
            }
        }
    }

    private static class DescComparator extends WritableComparator {
        public DescComparator() {
            super(LongWritable.class , true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration , "DesvcSort");

        job.setJarByClass(DescSort.class);
        job.setMapperClass(DescMapper.class);
        job.setReducerClass(DescReduce.class);
        job.setSortComparatorClass(DescComparator.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job , new Path("test/descSort/input"));
        FileOutputFormat.setOutputPath(job, new Path("test/descSort/output"));

        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(new Path("test/descSort/output"))) {
            fs.delete(new Path("test/descSort/output") , true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }

}
