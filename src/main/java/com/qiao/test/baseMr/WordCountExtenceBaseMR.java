package com.qiao.test.baseMr;

import com.qiao.test.wordCount.WordCoundLocalDirver;
import com.qiao.test.wordCount.WordCountCombiner;
import com.qiao.test.wordCount.WordCountMapper;
import com.qiao.test.wordCount.WordCountRudece;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountExtenceBaseMR extends BaseMR {
    @Override
    public Job getJob() throws IOException {
        Job job = Job.getInstance(getConfiguration(), getJobNameWithTaskId());

        job.setJarByClass(WordCountExtenceBaseMR.class);

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountCombiner.class);
        job.setReducerClass(WordCountRudece.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job,getOutputPath(getJobNameWithTaskId()));

        return job;
    }

    @Override
    public String getJobName() {
        return "WordCount";
    }
}
