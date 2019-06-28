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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 此练习为InnerJoin的编写
 *
 * @author qcy
 */
public class InnerJoinDiver extends Configured implements Tool {

    public static class InnerJoinMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        String type;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String path = fileSplit.getPath().toString();
            if (path.contains("m1.txt")) {
                type = "m1";
            } else {
                type = "m2";
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arrstr = value.toString().split(" ");

            if (arrstr.length != 2) {
                context.getCounter("userCounter", "error").increment(1L);
            }
            String id = arrstr[0];
            String str = arrstr[1];
            context.write(new LongWritable(Long.parseLong(id)), new Text(type + "==" + str));
        }
    }

    public static class InnerJoinReuded extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> list1 = new ArrayList<>();
            List<String> list2 = new ArrayList<>();
            for (Text value : values) {
                String[] arrStr = value.toString().split("==");
                String type = arrStr[0];
                String str = arrStr[1];
                if ("m1".equals(type)) {
                    list1.add(str);
                } else {
                    list2.add(str);
                }
            }
            Text valueOut = new Text();
            for (String s1:
                 list1) {
                for (String s2:
                     list2) {
                    valueOut.set(s1 + "\t" + s2);
                    context.write(key , valueOut);
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // 获取configuration对象
        Configuration conf = getConf();


        // 创建job对象
        Job job = Job.getInstance(conf, "wordcount");

        // job.setjarby
        job.setJarByClass(InnerJoinDiver.class);

        // job.setmapclass
        job.setMapperClass(InnerJoinMapper.class);

        // job.setmapoutputkeyclass
        job.setMapOutputKeyClass(LongWritable.class);

        //job.setmapoutputvalueclass
        job.setMapOutputValueClass(Text.class);

        // job.setreduceclass
        job.setReducerClass(InnerJoinReuded.class);

        // job.setoutputkeyclass
        job.setOutputKeyClass(LongWritable.class);
        // job.setoutputvalueclass
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path("test/innerJoin/output/"))) {
            fs.delete(new Path("test/innerJoin/output/"), true);
        }

        FileInputFormat.setInputPaths(job, new Path("test/innerJoin/input/"));
        FileOutputFormat.setOutputPath(job, new Path("test/innerJoin/output/"));

        boolean status = job.waitForCompletion(true);

        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new InnerJoinDiver(), args));
    }
}
