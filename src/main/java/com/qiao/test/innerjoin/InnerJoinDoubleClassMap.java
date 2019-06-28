package com.qiao.test.innerjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InnerJoinDoubleClassMap extends Configured implements Tool {

    private static class JoinFirstMapper extends Mapper<LongWritable, Text, Text, Text> {
        String type = "m1";

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = value.toString().split(" ");
            if (strArr.length != 2) {
                context.getCounter("user", "error").increment(1L);
                return;
            } else {
                context.write(new Text(strArr[0]), new Text(strArr[1] + "===" + type));
            }

        }
    }

    private static class JoinSecondMapper extends Mapper<LongWritable, Text, Text, Text> {
        String type = "m2";

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = value.toString().split(" ");
            if (strArr.length != 2) {
                context.getCounter("user", "error").increment(1L);
                return;
            } else {
                context.write(new Text(strArr[0]), new Text(strArr[1] + "===" + type));
            }
        }
    }

    private static class JoinDoubleReduce extends Reducer<Text, Text, Text, Text> {
        List<String> list1 = new ArrayList<>();
        List<String> list2 = new ArrayList<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            list1.clear();
            list2.clear();
            for (Text value :
                    values) {
                String[] strArr = value.toString().split("===");
                if ("m1".equals(strArr[1])) {
                    list1.add(strArr[0]);
                } else {


                    list2.add(strArr[0]);
                }
            }
            for (String m1 : list1) {
                for (String m2 : list2) {
                    context.write(key, new Text(m1 + "\t" + m2));
                }
            }


        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        Job job = Job.getInstance(configuration, "InnerJoinDoubleClassMap");

        job.setJarByClass(InnerJoinDoubleClassMap.class);

        job.setReducerClass(JoinDoubleReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path("test/innerJoin/input/m1.txt"), TextInputFormat.class, JoinFirstMapper.class);
        MultipleInputs.addInputPath(job, new Path("test/innerJoin/input/m2.txt"), TextInputFormat.class, JoinSecondMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("test/innerJoin/output_Multiple"));
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(new Path("test/innerJoin/output_Multiple"))) {
            fs.delete(new Path("test/innerJoin/output_Multiple"), true);
        }


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new InnerJoinDoubleClassMap(), args));
    }
}
