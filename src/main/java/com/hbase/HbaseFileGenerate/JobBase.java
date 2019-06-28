package com.hbase.HbaseFileGenerate;

import com.qiao.test.baseMr.BaseMR;
import com.qiao.test.baseMr.MaxWordCountExtenceBase;
import com.qiao.test.baseMr.WordCountExtenceBaseMR;
import com.qiao.test.jobLink.maxWordCount.WordMaxJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;

public class JobBase extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        System.out.println("======= run ===========");

        /** 一、获取配置对象 */
        Configuration configuration = this.getConf();
        BaseMR.init(configuration);
        /** 二、创建JobContrllo*/
        JobControl jobControl = new JobControl("BaseWordMax");
        /** 三、创建任务链对象 ControlledJob*/
        FileGenerate fileGenerate = new FileGenerate();
        ControlledJob fileGenerateControlledJob = fileGenerate.getControlledJob();
        /** 四、给任务链中的任务添加任务依赖*/

        /** 五、将任务添加至任务链中*/
        jobControl.addJob(fileGenerateControlledJob);

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

//        String[] parameter = {"-Dtask.id=0412_qiao", "-Dtask.input.dir=data/input", "-Dtask.base.dir=data/output" , "-Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181"};

        System.exit(ToolRunner.run(new JobBase(), args));
    }

}
