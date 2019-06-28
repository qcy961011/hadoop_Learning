package com.qiao.test.jobLink.maxWordCount;

import com.qiao.test.maxWordCount.MaxWordCountDiver;
import com.qiao.test.maxWordCount.MaxWordCountMapper;
import com.qiao.test.maxWordCount.MaxWordCountReducer;
import com.qiao.test.wordCount.WordCoundLocalDirver;
import com.qiao.test.wordCount.WordCountCombiner;
import com.qiao.test.wordCount.WordCountMapper;
import com.qiao.test.wordCount.WordCountRudece;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

public class WordMaxJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        /** 一、获取配置对象 */
        Configuration configuration = this.getConf();
        /** 二、创建JobContrllo*/
        JobControl jobControl = new JobControl("wordMax");
        /** 三、创建任务链对象 ControlledJob*/
        // WordCount 的 ControlledJob
        ControlledJob wcContrJob = new ControlledJob(configuration);
        Job wcJob = getWcJob(configuration, args);
        /**-------------将jobControl和job关联---------------- */
        wcContrJob.setJob(wcJob);

        // MaxWord 的 ControlledJob
        ControlledJob mwContrJob = new ControlledJob(configuration);
        Job maxJob = getMaxJob(configuration, args);
        mwContrJob.setJob(maxJob);

        /** 四、给任务链中的任务添加任务依赖*/
        // mwJob --> wcJob
        mwContrJob.addDependingJob(wcContrJob);
        /** 五、将任务添加至任务链中*/
        jobControl.addJob(wcContrJob);
        jobControl.addJob(mwContrJob);

        Thread thread = new Thread(() -> {
            long start = System.currentTimeMillis();
            while (!jobControl.allFinished()) {
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            long end = System.currentTimeMillis();

            List<ControlledJob> failedJobList = jobControl.getFailedJobList();
            if (failedJobList.isEmpty()) {
                System.out.println("all job run successed!");
            } else {
                System.out.println("part job run failed , detaill failed job name");
                for (ControlledJob job :
                        failedJobList) {
                    System.out.println(job.getJobName());
                }
            }

            jobControl.stop();
            System.out.println("运行时间 ： " + (end - start));
        });
        thread.start();

        /** 六、运行任务链*/
        jobControl.run();
        return 0;
    }

    private Job getMaxJob(Configuration configuration, String[] args) throws IOException {
        Job job = Job.getInstance(configuration, "MaxWordCount");

        FileSystem fs = FileSystem.get(configuration);
        job.setJarByClass(MaxWordCountDiver.class);

        job.setMapperClass(MaxWordCountMapper.class);
        job.setReducerClass(MaxWordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        if (fs.exists(new Path(args[2]))) {
            fs.delete(new Path(args[2]), true);
        }
        return job;
    }

    private Job getWcJob(Configuration configuration, String[] args) throws IOException {
        Job wcJob = Job.getInstance(configuration, "WordCount");

        FileSystem fs = FileSystem.get(configuration);
        wcJob.setJarByClass(WordCoundLocalDirver.class);

        wcJob.setMapperClass(WordCountMapper.class);
        wcJob.setCombinerClass(WordCountCombiner.class);
        wcJob.setReducerClass(WordCountRudece.class);

        wcJob.setMapOutputKeyClass(Text.class);
        wcJob.setMapOutputValueClass(IntWritable.class);

        wcJob.setOutputKeyClass(Text.class);
        wcJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(wcJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wcJob, new Path(args[1]));

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        return wcJob;
    }

    public static void main(String[] args) throws Exception {
        String[] parameter = {"test/wordCount/input", "test/wordCount/output", "test/wordCount/output_max"};

        System.exit(ToolRunner.run(new WordMaxJob(), parameter));
    }

}
