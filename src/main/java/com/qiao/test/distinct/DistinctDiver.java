package com.qiao.test.distinct;

import com.qiao.test.maxWordCount.MaxWordCountDiver;
import com.qiao.test.maxWordCount.MaxWordCountMapper;
import com.qiao.test.maxWordCount.MaxWordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DistinctDiver {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(configuration);

        Job job = Job.getInstance(configuration);

        job.setJarByClass(DistinctDiver.class);

        job.setMapperClass(DistinctMapper.class);
        job.setReducerClass(DistinctReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

//        job.setNumReduceTasks(3);
        FileInputFormat.setInputPaths(job, new Path("test/wordCount/input/"));
        FileOutputFormat.setOutputPath(job, new Path("test/wordCount/output/"));

        if (fs.exists(new Path("test/wordCount/output/"))) {
            fs.delete(new Path("test/wordCount/output/"), true);
        }

        boolean result = job.waitForCompletion(true);
        String msg = result ? "命令执行成功" : "命令执行失败";
        System.out.println(msg);
        System.exit(result ? 0 : -1);
    }

}
