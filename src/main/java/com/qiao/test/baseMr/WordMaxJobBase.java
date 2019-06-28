package com.qiao.test.baseMr;

import com.qiao.test.jobLink.maxWordCount.WordMaxJob;
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
import java.util.List;

public class WordMaxJobBase extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        /** 一、获取配置对象 */
        Configuration configuration = this.getConf();
        BaseMR.init(configuration);
        /** 二、创建JobContrllo*/
        JobControl jobControl = new JobControl("BaseWordMax");
        /** 三、创建任务链对象 ControlledJob*/
        WordCountExtenceBaseMR countExtenceBaseMR = new WordCountExtenceBaseMR();
        ControlledJob countExtenceBaseMRControlledJob = countExtenceBaseMR.getControlledJob();
        /**-------------将jobControl和job关联---------------- */
        MaxWordCountExtenceBase maxWordCountExtenceBase = new MaxWordCountExtenceBase();
        ControlledJob maxWordCountExtenceBaseControlledJob = maxWordCountExtenceBase.getControlledJob();
        /** 四、给任务链中的任务添加任务依赖*/
        // mwJob --> wcJob
        maxWordCountExtenceBaseControlledJob.addDependingJob(countExtenceBaseMRControlledJob);
        /** 五、将任务添加至任务链中*/
        jobControl.addJob(countExtenceBaseMRControlledJob);
        jobControl.addJob(maxWordCountExtenceBaseControlledJob);

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

    public static void main(String[] args) throws Exception {

        String[] parameter = {"test/wordCount/input", "test/wordCount/output", "test/wordCount/output_max"};

        System.exit(ToolRunner.run(new WordMaxJob(), parameter));
    }

}
