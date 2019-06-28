package com.qiao.test.distinct;

import jdk.nashorn.internal.scripts.JO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DistinctDiverSelf extends Configured implements Tool {

    private static class DistincMapper extends Mapper<LongWritable , Text , Text , NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = value.toString().split(" ");
            for (String str:
                 strArr) {
                context.write(new Text(str) , NullWritable.get());
            }
        }
    }

    private static class DistincReduce extends Reducer<Text , NullWritable , Text , NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        FileSystem fileSystem = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration , "distinct");

        job.setJarByClass(DistinctDiverSelf.class);

        job.setMapperClass(DistinctMapper.class);
        job.setReducerClass(DistinctReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job , new Path("test/wordCount/input/"));
        FileOutputFormat.setOutputPath(job , new Path("test/wordCount/output/"));

        if (fileSystem.exists(new Path("test/wordCount/output/"))) {
            fileSystem.delete(new Path("test/wordCount/output/"), true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DistinctDiverSelf(),args));
    }
}
