package com.qiao.test.score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Partitioner 练习
 *
 * @author 10508
 */
public class ScoreDiver {

    private static class ScoreMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = value.toString().split(" ");
            if (strArr.length != 2) {
                context.getCounter("user" , "error").increment(1);
                return;
            }
            context.write(new Text(strArr[0]) , new LongWritable(Long.parseLong(strArr[1])));


        }
    }

    private static class ScoreReduce extends Reducer<Text, LongWritable , Text , LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable value:
                 values) {
                context.write(key , value);
            }
        }
    }

    private static class ScorePartitioner extends Partitioner<Text , LongWritable> {

        @Override
        public int getPartition(Text text, LongWritable longWritable, int numPartitions) {
            if(longWritable.get() > 60) {
                return 0;
            } else {
                return 1;
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration , "Score");
        job.setJarByClass(ScoreDiver.class);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReduce.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setPartitionerClass(ScorePartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job , new Path("test/score/input"));
        Path outPath = new Path("test/score/output");
        FileOutputFormat.setOutputPath(job , outPath);

        FileSystem fs = FileSystem.get(configuration);

        if (fs.exists(outPath)) {
            fs.delete(outPath , true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : -1);

    }
}
