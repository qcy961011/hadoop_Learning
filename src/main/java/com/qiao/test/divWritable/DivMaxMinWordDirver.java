package com.qiao.test.divWritable;

import com.hainiuxy.mapreduce.WordCountWithCompress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DivMaxMinWordDirver extends Configured implements Tool {

    public static class DivMaxMinWordMapper extends Mapper<LongWritable , Text , Text , WordWritable>{

        Text keyOut = new Text();

        WordWritable valueOut = new WordWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arrStr = value.toString().split(" ");

            String name = arrStr[0];
            long num = Long.parseLong(arrStr[1]);

            keyOut.set(name);
            valueOut.setNum(num);
            context.write(keyOut , valueOut);
        }
    }


    public static class DivMaxMinWordReduce extends Reducer<Text , WordWritable , Text , Text> {

        Text valueOut = new Text();

        @Override
        protected void reduce(Text key, Iterable<WordWritable> values, Context context) throws IOException, InterruptedException {

            long max = 0L;
            long min = Long.MAX_VALUE;

            for (WordWritable value:
                 values) {
                if (max <= value.getNum()) {
                    max = value.getNum();
                }
                if (min >= value.getNum()) {
                    min = value.getNum();
                }
            }
            valueOut.set("max :" + max + "  min : " + min);
            context.write(key , valueOut);
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        // 获取configuration对象
        Configuration conf = getConf();


        // 创建job对象
        Job job = Job.getInstance(conf, "wordcount");

        // job.setjarby
        job.setJarByClass(DivMaxMinWordDirver.class);

        // job.setmapclass
        job.setMapperClass(DivMaxMinWordMapper.class);

        // job.setmapoutputkeyclass
        job.setMapOutputKeyClass(Text.class);

        //job.setmapoutputvalueclass
        job.setMapOutputValueClass(WordWritable.class);

        // job.setreduceclass
        job.setReducerClass(DivMaxMinWordReduce.class);

        // job.setoutputkeyclass
        job.setOutputKeyClass(Text.class);
        // job.setoutputvalueclass
        job.setOutputValueClass(Text.class);

        // 设置 输入目录
        FileInputFormat.addInputPath(job, new Path("test/wordCount/input/"));

        // 设置输出目录
        Path outputDir = new Path("test/wordCount/output/");
        FileOutputFormat.setOutputPath(job, outputDir);
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path("test/wordCount/output/"))) {
            fs.delete(new Path("test/wordCount/output/"), true);
        }

        FileInputFormat.setInputPaths(job, new Path("test/wordCount/input/"));
        FileOutputFormat.setOutputPath(job, new Path("test/wordCount/output/"));

        boolean status = job.waitForCompletion(true);

        return status? 0 : 1;
    }



    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DivMaxMinWordDirver(), args));
    }

}
