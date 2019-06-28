package com.qiao.test.wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.BigDecimal;

public class WordCoundLocalDirver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

//        configuration.set(MRJobConfig.MAP_OUTPUT_COMPRESS, "true");
        configuration.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class.getName());

        FileSystem fs = FileSystem.get(configuration);

        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCoundLocalDirver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountCombiner.class);
        job.setReducerClass(WordCountRudece.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        job.setNumReduceTasks(3);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        boolean result = job.waitForCompletion(true);
        String msg = result ? "命令执行成功" : "命令执行失败";
        System.out.println(msg);
        System.exit(result ? 0 : -1);

    }

}
