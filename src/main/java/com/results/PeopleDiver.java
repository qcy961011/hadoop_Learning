package com.results;

import com.qiao.test.baseMr.BaseMR;
import com.results.number.NumberDiver;
import com.results.sex.SexDiver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;

public class PeopleDiver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("task.input.dir" , args[0]);
        configuration.set("task.base.dir" , args[1]);
        configuration.set("task.id" , args[2]);

        BaseMR.init(configuration);

        JobControl jobControl = new JobControl("People");

        SexDiver sexDiver = new SexDiver();
        ControlledJob sexJob = sexDiver.getControlledJob();

        NumberDiver numberDiver = new NumberDiver();
        ControlledJob numberJob = numberDiver.getControlledJob();

        numberJob.addDependingJob(sexJob);

        jobControl.addJob(sexJob);
        jobControl.addJob(numberJob);



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
        System.exit(ToolRunner.run(new PeopleDiver() ,args));

    }
}
