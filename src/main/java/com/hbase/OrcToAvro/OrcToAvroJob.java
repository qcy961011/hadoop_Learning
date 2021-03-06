package com.hbase.OrcToAvro;

import com.teacher.util.JobRunResult;
import com.teacher.util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OrcToAvroJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        JobControl jobControl = new JobControl("OrcToAvroJob");

        OrcToAvro avro = new OrcToAvro();
        avro.setConf(configuration);

        ControlledJob avroContrJob = avro.getControlledJob();

        jobControl.addJob(avroContrJob);
        JobRunResult result = JobRunUtil.run(jobControl);
        result.print(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new OrcToAvroJob() , args));
    }
}
