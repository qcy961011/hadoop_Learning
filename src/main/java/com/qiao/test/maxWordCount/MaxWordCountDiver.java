package com.qiao.test.maxWordCount;

import com.qiao.test.wordCount.WordCoundLocalDirver;
import com.qiao.test.wordCount.WordCountCombiner;
import com.qiao.test.wordCount.WordCountMapper;
import com.qiao.test.wordCount.WordCountRudece;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxWordCountDiver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();


        Job job = Job.getInstance(configuration);

        FileSystem fs = FileSystem.get(configuration);
        job.setJarByClass(MaxWordCountDiver.class);

        job.setMapperClass(MaxWordCountMapper.class);
        job.setReducerClass(MaxWordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("test/wordCount/input/"));
        FileOutputFormat.setOutputPath(job, new Path("test/wordCount/output_test/"));

        if (fs.exists(new Path("test/wordCount/output_test/"))) {
            fs.delete(new Path("test/wordCount/output_test/"), true);
        }

        boolean result = job.waitForCompletion(true);
        String msg = result ? "命令执行成功" : "命令执行失败";
        System.out.println(msg);
        System.exit(result ? 0 : -1);
    }

}
