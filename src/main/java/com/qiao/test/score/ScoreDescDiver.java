package com.qiao.test.score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ScoreDescDiver {

    private static class ScoreDescMapper extends Mapper<LongWritable , Text , LongWritable , Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = value.toString().split(" ");
            if (strArr.length != 2) {
                context.getCounter("user" , "error").increment(1);
                return;
            }
            context.write(new LongWritable(Long.parseLong(strArr[1])) , new Text(strArr[0]));
        }
    }

    private static class ScoreDescReducer extends Reducer< LongWritable , Text ,Text ,LongWritable> {

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value:
                 values) {
                context.write(value , key);
            }
        }
    }

    /**
     * 手动设置分区方式
     */
    private static class ScoreDescPartitioner extends Partitioner<LongWritable,Text> {
        @Override
        public int getPartition(LongWritable longWritable, Text text, int numPartitions) {
            if(longWritable.get() >= 60) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    /**
     * 手动设置排序方式
     */
    private static class ScoreDescComparator extends WritableComparator {

        public ScoreDescComparator() {
            super(LongWritable.class);
        }

        @Override
        public int compare(Object a, Object b) {
            return -super.compare(a, b);
        }
    }




    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration , "Score");
        job.setJarByClass(ScoreDescDiver.class);
        job.setMapperClass(ScoreDescMapper.class);
        job.setReducerClass(ScoreDescReducer.class);

        job.setSortComparatorClass(ScoreDescComparator.class);
        job.setPartitionerClass(ScoreDescPartitioner.class);


        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job , new Path("test/scoreDesc/input"));
        Path outPath = new Path("test/scoreDesc/output");
        FileOutputFormat.setOutputPath(job , outPath);

        FileSystem fs = FileSystem.get(configuration);

        if (fs.exists(outPath)) {
            fs.delete(outPath , true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : -1);

    }
}
