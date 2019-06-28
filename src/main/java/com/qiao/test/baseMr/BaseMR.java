package com.qiao.test.baseMr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import java.io.IOException;

/**
 * 任务链公共基类
 */
public abstract class BaseMR {

    private static Configuration configuration = null;

    public static void init(Configuration configuration) {
        BaseMR.configuration = configuration;
    }

    public static Configuration getConfiguration() {
        return configuration;
    }

    public static void setConfiguration(Configuration configuration) {
        BaseMR.configuration = configuration;
    }

    public ControlledJob getControlledJob() throws IOException {

        // 创建任务链中的ControlledJob对象
        ControlledJob controlledJob = new ControlledJob(configuration);

        // 生成Job
        Job job = getJob();

        // 自动删除生成目录
        FileSystem fs = FileSystem.get(configuration);
        Path output = getOutputPath(getJobNameWithTaskId());
        if (fs.exists(output)) {
            fs.delete(output,true);
        }

        // 将任务对象和ControlledJob进行关联
        controlledJob.setJob(job);
        return controlledJob;
    }

    public abstract Job getJob() throws IOException;

    public abstract String getJobName();


    /**
     * 获取个性化任务名
     * @return
     */
    public String getJobNameWithTaskId() {
        return getJobName() + "_" + configuration.get("task.id");
    }

    /**
     * 获取首个任务的输入目录
     * @return
     */
    public Path getFirstJobInputPath() {
        return new Path(configuration.get("task.input.dir"));
    }

    /**
     * 任务输出目录
     * @param jobName
     * @return
     */
    public Path getOutputPath(String jobName) {
        return new Path(configuration.get("task.base.dir") + "/" + jobName);
    }

}
