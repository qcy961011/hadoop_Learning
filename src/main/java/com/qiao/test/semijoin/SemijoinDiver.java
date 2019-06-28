package com.qiao.test.semijoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class SemijoinDiver extends Configured implements Tool {

    private static class SemijoinMapper extends Mapper<LongWritable, Text , Text , Text>{
        Map<String, String> cacheMap = new HashMap<>();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles.length != 1) {
                return;
            }
            String path = cacheFiles[0].getPath().toString();
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            String line = "";
            while ((line = reader.readLine()) != null) {
                String[] strArr = line.split(" ");
                String key = strArr[0];
                String value = strArr[1];
                cacheMap.put(key , value);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = value.toString().split(" ");
            if (strArr.length != 2) {
                context.getCounter("user","error").increment(1);
                return;
            }
            context.write(new Text(strArr[0]) , new Text(cacheMap.get(strArr[1]) + "\t" + strArr[1]));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        configuration.set(MRJobConfig.CACHE_FILES,args[0]);
        Job job = Job.getInstance(configuration , "semiJoinDiver");
        job.setJarByClass(SemijoinDiver.class);
        job.setMapperClass(SemijoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job , new Path(args[1]));
        Path output = new Path(args[2]);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(output)) {
            fs.delete(output , true);
        }
        FileOutputFormat.setOutputPath(job, output);


        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new SemijoinDiver() , args));
    }
}
