package com.qiao.test.teacherHomeWork.diver;

import com.qiao.test.baseMr.BaseMR;
import com.qiao.test.teacherHomeWork.max.MaxReslut;
import com.qiao.test.teacherHomeWork.sumReslut.SumReslut;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;

public class JobLinkDiver extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = this.getConf();

        BaseMR.init(configuration);

        JobControl jc = new JobControl( "maxReslut");

        SumReslut sumReslut = new SumReslut();
        ControlledJob sumJob = sumReslut.getControlledJob();
        MaxReslut maxReslut = new MaxReslut();
        ControlledJob maxJob = maxReslut.getControlledJob();

        maxJob.addDependingJob(sumJob);

        jc.addJob(sumJob);
        jc.addJob(maxJob);



        Thread thread = new Thread(() -> {
            long start = System.currentTimeMillis();
            while (!jc.allFinished()) {
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            long end = System.currentTimeMillis();

            List<ControlledJob> failedJobList = jc.getFailedJobList();
            if (failedJobList.isEmpty()) {
                System.out.println("all job run successed!");
            } else {
                System.out.println("part job run failed , detaill failed job name");
                for (ControlledJob job :
                        failedJobList) {
                    System.out.println(job.getJobName());
                }
            }

            jc.stop();
            System.out.println("运行时间 ： " + (end - start));
        });
        thread.start();

        /** 六、运行任务链*/
        jc.run();
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new JobLinkDiver() , args));
    }
}
