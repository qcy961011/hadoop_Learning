package com.qiao.test.wordCount.test;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class InnerJoinDiverSelf extends Configured implements Tool {

    private static class InnerJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        String type;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String path = fileSplit.getPath().toString();
            type = path.substring(path.lastIndexOf("/") + 1);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arrStr = value.toString().split(" ");
            if (arrStr.length != 2) {
                context.getCounter("userCounter", "error").increment(1);
                return;
            }
            Text outValue = new Text(type + "====" + arrStr[1]);
            context.write(new Text(arrStr[0]) , outValue);
        }
    }

    private static class InnerJoinReuded extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, List<String>> reslut = new HashMap<>();
            for (Text value : values) {
                String[] arrStr = value.toString().split("====");
                if (reslut.get(arrStr[0]) != null) {
                    reslut.get(arrStr[0]).add(arrStr[1]);
                } else {
                    List list = new ArrayList<>();
                    list.add(arrStr[1]);
                    reslut.put(arrStr[0], list);
                }
            }
            Iterator iterator = reslut.entrySet().iterator();
            StringBuffer str = new StringBuffer();
            addString(str , iterator , context , key);
        }

        private void addString(StringBuffer str, Iterator iterator, Context context, Text key) throws IOException, InterruptedException {

            if (iterator.hasNext()) {
                HashMap.Entry entry = (HashMap.Entry) iterator.next();
                List<String> list = (List) entry.getValue();
                for (String value : list) {
                    str.append(value + "\t");
                    addString(str, iterator, context , key);
                }
            } else {
                context.write(key , new Text(str.toString()));
            }
            return;
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        // 获取configuration对象
        Configuration conf = getConf();


        // 创建job对象
        Job job = Job.getInstance(conf, "InnerJoin");

        job.setJarByClass(InnerJoinDiverSelf.class);

        job.setMapperClass(InnerJoinMapper.class);
        job.setReducerClass(InnerJoinReuded.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
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
        System.exit(ToolRunner.run(new InnerJoinDiverSelf(), args));
    }
}
