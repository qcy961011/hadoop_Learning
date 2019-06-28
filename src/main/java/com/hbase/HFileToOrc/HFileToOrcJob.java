package com.hbase.HFileToOrc;

import com.util.JobRunResult;
import com.util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HFileToOrcJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
//		1）获取配置对象
        Configuration conf = getConf();

//		2）创建任务链对象 JobControl
        JobControl jobc = new JobControl("HFile2OrcJob");

//		3）创建任务链中要添加的任务对象  ControlledJob
        HFileToOrc orc = new HFileToOrc();
        orc.setConf(conf);

        ControlledJob orcCJob = orc.getControlledJob();


//		5）将任务添加到任务链中
        jobc.addJob(orcCJob);

        JobRunResult result = JobRunUtil.run(jobc);
        result.print(true);

        return 0;

    }


    public static void main(String[] args) throws Exception {
//		-Dtask.id=0416 -Dtask.input.dir=/tmp/hbase/input_hfile -Dtask.base.dir=/tmp/hbase/output
        System.exit(ToolRunner.run(new HFileToOrcJob(), args));
    }
}
