package com.test;

import com.qiao.test.sort.OneKeyDiver;
import com.util.JsonUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

public class TestMapReduce extends Configured implements Tool {

    private static class OneKeyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            line = line.replace("\\", "");
            Map map = JsonUtil.readJson(line);
            context.write(new Text(map.toString()), NullWritable.get());
        }

    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();
        FileSystem fs = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration);

        job.setMapperClass(OneKeyMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        if (fs.exists(new Path("data/test/output/"))) {
            fs.delete(new Path("data/test/output/"), true);
        }

        FileInputFormat.setInputPaths(job, new Path("data/test/input"));
        FileOutputFormat.setOutputPath(job, new Path("data/test/output/"));

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TestMapReduce(), args));
    }

}
