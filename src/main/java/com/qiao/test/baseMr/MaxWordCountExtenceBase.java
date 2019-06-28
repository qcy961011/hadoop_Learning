package com.qiao.test.baseMr;

import com.qiao.test.maxWordCount.MaxWordCountDiver;
import com.qiao.test.maxWordCount.MaxWordCountMapper;
import com.qiao.test.maxWordCount.MaxWordCountReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxWordCountExtenceBase extends BaseMR {
    @Override
    public Job getJob() throws IOException {
        Job job = Job.getInstance(getConfiguration() , getJobNameWithTaskId());
        job.setJarByClass(MaxWordCountExtenceBase.class);

        job.setMapperClass(MaxWordCountMapper.class);
        job.setReducerClass(MaxWordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        WordCountExtenceBaseMR wc = new WordCountExtenceBaseMR();

        FileInputFormat.setInputPaths(job, wc.getOutputPath( wc.getJobNameWithTaskId()));
        Path output = getOutputPath(getJobNameWithTaskId());
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    @Override
    public String getJobName() {
        return "maxWord";
    }
}
