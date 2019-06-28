package com.qiao.test.divMaxAndMinSaveToLocal;

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

import java.io.IOException;

public class DivMaxAndMinSaveToLocal extends Configured implements Tool {

    private static class MaxAndMinMapper extends Mapper<LongWritable, Text, Text, WordWritable> {

        MultipleOutputs<Text, WordWritable> outputs = null;

        Text keyOut = new Text();

        WordWritable valueOut = new WordWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outputs = new MultipleOutputs<Text, WordWritable>(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strArr = value.toString().split(" ");
            if (strArr.length != 2) {
                context.getCounter("user", "error").increment(1L);
                return;
            }
            keyOut.set(strArr[0]);
            valueOut.setNumber(Integer.parseInt(strArr[1]));
            valueOut.setWord(keyOut);
//            valueOut.setType("default");
            context.write(keyOut, valueOut);
            outputs.write(keyOut, valueOut, "mapout");
        }
        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, WordWritable>.Context context)
                throws IOException, InterruptedException {
            outputs.close();

        }
    }

    private static class MaxAndMinCombiner extends Reducer<Text, WordWritable, Text, WordWritable> {
        IntWritable max = new IntWritable(Integer.MIN_VALUE);
        IntWritable min = new IntWritable(Integer.MAX_VALUE);

        @Override
        protected void reduce(Text key, Iterable<WordWritable> values, Context context) throws IOException, InterruptedException {
            for (WordWritable value:
                 values) {
                if(value.getNumber() > max.get()) {
                    max.set(value.getNumber());
                }
                if(value.getNumber() < min.get()) {
                    min.set(value.getNumber());
                }
            }
            WordWritable valueOutmax = new WordWritable();
//            valueOutmax.setType("max");
            valueOutmax.setNumber(max.get());
            valueOutmax.setWord(key);
            context.write(key , valueOutmax);
            WordWritable valueOutmin = new WordWritable();
//            valueOutmin.setType("min");
            valueOutmin.setNumber(min.get());
            valueOutmin.setWord(key);
            context.write(key , valueOutmin);
        }
    }

    private static class MaxAndMinReduce extends Reducer<Text, WordWritable, Text, StringWirtable>{
        StringWirtable stringWirtable = new StringWirtable();
        MultipleOutputs<Text, StringWirtable> outputs = null;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            outputs = new MultipleOutputs<Text, StringWirtable>(context);
        }
        @Override
        protected void reduce(Text key, Iterable<WordWritable> values, Context context) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            for (WordWritable value:
                    values) {
                if(value.getNumber() > max) {
                    max = value.getNumber();
                }
                if(value.getNumber() < min) {
                    min = value.getNumber();
                }
            }
            stringWirtable.setSum(max);
            context.write(key , stringWirtable);
            outputs.write(key , stringWirtable , "maxout/max");
            stringWirtable.setSum(min);
            context.write(key , stringWirtable);
            outputs.write(key , stringWirtable , "minout/min");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();
        Job job = Job.getInstance(configuration , "DivMaxAndMinSaveToLocal");
        FileSystem fileSystem = FileSystem.get(configuration);

        job.setJarByClass(DivMaxAndMinSaveToLocal.class);

        job.setMapperClass(MaxAndMinMapper.class);
        job.setCombinerClass(MaxAndMinCombiner.class);
        job.setReducerClass(MaxAndMinReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WordWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StringWirtable.class);

        FileInputFormat.setInputPaths(job , new Path("test/definition/input/"));
        FileOutputFormat.setOutputPath(job , new Path("test/definition/output/"));

        if (fileSystem.exists(new Path("test/definition/output/"))) {
            fileSystem.delete(new Path("test/definition/output/"), true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DivMaxAndMinSaveToLocal() , args));
    }


}
