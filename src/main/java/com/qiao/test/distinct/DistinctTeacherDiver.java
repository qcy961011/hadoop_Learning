package com.qiao.test.distinct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistinctTeacherDiver extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();
        FileSystem fs = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration);

        job.setJarByClass(DistinctTeacherDiver.class);

        job.setMapperClass(DistinctMapper.class);
        job.setReducerClass(DistinctReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        if (fs.exists(new Path("test/wordCount/output/"))) {
            fs.delete(new Path("test/wordCount/output/"), true);
        }

        FileInputFormat.setInputPaths(job, new Path("test/wordCount/input/"));
        FileOutputFormat.setOutputPath(job, new Path("test/wordCount/output/"));

        boolean status = job.waitForCompletion(true);


        return status? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DistinctTeacherDiver() , args));
    }


}
