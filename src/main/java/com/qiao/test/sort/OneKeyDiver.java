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
import java.util.HashSet;
import java.util.Set;

public class OneKeyDiver extends Configured implements Tool {

    private static class OneKeyMapper extends Mapper<LongWritable, Text, Text, Text> {
        long max = 0;
        long min = Long.MAX_VALUE;

        private Text maxKey = new Text("Max");
        private Text maxValue = new Text();
        private Text minKey = new Text("Min");
        private Text minValue = new Text();


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arrStr = value.toString().split(" ");
            if (arrStr.length != 2) {
                context.getCounter("userGroup", "error").increment(1L);
                return;
            }
            if (Long.parseLong(arrStr[1]) > max) {
                max = Long.parseLong(arrStr[1]);
                maxValue.set(arrStr[0] + "===" + arrStr[1]);
            }
            if (Long.parseLong(arrStr[1]) < min) {
                min = Long.parseLong(arrStr[1]);
                minValue.set(arrStr[0] + "===" + arrStr[1]);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(maxKey, maxValue);
            context.write(minKey, minValue);
        }
    }

    private static class OntKeyReduce extends Reducer<Text, Text, Text, Text> {
        private Text overValue = new Text();
        long max = 0;
        long min = Long.MAX_VALUE;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if ("Max".equals(key.toString())) {
                for (Text value : values) {
                    String[] arrStr = value.toString().split("===");
                    if (max < Long.parseLong(arrStr[1])) {
                        overValue = value;
                        max = Long.parseLong(arrStr[1]);
                    }
                }
            } else {
                for (Text value : values) {
                    String[] arrStr = value.toString().split("===");
                    if (min > Long.parseLong(arrStr[1])) {
                        overValue = value;
                        min = Long.parseLong(arrStr[1]);
                    }
                }
            }
            context.write(key , overValue);

        }
    }


    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();
        FileSystem fs = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration);

        job.setJarByClass(SortDiver.class);

        job.setMapperClass(OneKeyMapper.class);
        job.setReducerClass(OntKeyReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (fs.exists(new Path("test/sort/output/"))) {
            fs.delete(new Path("test/sort/output/"), true);
        }

        FileInputFormat.setInputPaths(job, new Path("test/sort/input/"));
        FileOutputFormat.setOutputPath(job, new Path("test/sort/output/"));

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new OneKeyDiver(), args));
    }

}
