package com.qiao.test.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SortDiver extends Configured implements Tool {

    public static class SortMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text lastKey = new Text();
        private int max = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = value.toString().split(" ");
            if (strArr.length > 2) {
                context.getCounter("userGroup", "error").increment(1L);
                return;
            }
            if (Integer.parseInt(strArr[1]) > max) {
                max = Integer.parseInt(strArr[1]);
                lastKey.set(strArr[0]);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(lastKey, new IntWritable(max));
        }
    }

    public static class SortReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text lastKey = new Text();
        private int max = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value :
                    values) {
                if (max < value.get()) {
                    lastKey = key;
                    max = value.get();
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(lastKey, new IntWritable(max));
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();
        FileSystem fs = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration);

        job.setJarByClass(SortDiver.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (fs.exists(new Path("test/sort/output/"))) {
            fs.delete(new Path("test/sort/output/"), true);
        }

        FileInputFormat.setInputPaths(job, new Path("test/sort/input/"));
        FileOutputFormat.setOutputPath(job, new Path("test/sort/output/"));

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SortDiver(), args));
    }

}
