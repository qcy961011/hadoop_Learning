package com.hbase.orcToText;

import com.teacher.util.JobRunResult;
import com.teacher.util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OrcToTextJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        JobControl jobControl = new JobControl("OrcToText");

        OrcToText orc = new OrcToText();
        orc.setConf(configuration);
        ControlledJob controlledJob = orc.getControlledJob();
        jobControl.addJob(controlledJob);
        JobRunResult result = JobRunUtil.run(jobControl);
        result.print(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new OrcToTextJob() , args));
    }
}
