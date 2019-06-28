package com.self;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.impl.IntegerWriter;

import java.io.IOException;

public class SqlMapperDiver extends Configured implements Tool {

    private static class SqlMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        MultipleOutputs<Text, IntWritable> outputs = null;

        @Override
        protected void setup(Context context){
            outputs = new MultipleOutputs<Text, IntWritable>(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = value.toString().split("\124");
            for (String str :
                    strArr) {
                char[] chars = str.toCharArray();
            }
        }
    }


    private static class SqlReduce extends Reducer<Text, IntegerWriter, Text, IntWritable> {

    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        Job job = Job.getInstance(configuration, "sql");
        FileSystem fileSystem = FileSystem.get(configuration);

        job.setJarByClass(SqlMapperDiver.class);

        job.setMapperClass(SqlMapper.class);
        job.setReducerClass(SqlReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("test/sql/input/"));
        FileOutputFormat.setOutputPath(job, new Path("test/sql/output/"));

        if (fileSystem.exists(new Path("test/sql/output/"))) {
            fileSystem.delete(new Path("test/sql/output/"), true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SqlMapperDiver(), args));
    }

}
