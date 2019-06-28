package com.hbase.test;

import com.teacher.util.JobRunResult;
import com.teacher.util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Orc2HfileJob extends Configured implements Tool {
    @Override
    public int run(String [] args) throws Exception {

        Configuration conf=getConf();
        JobControl jobc = new JobControl("Orc2HfileJob");


        Orc2Hfile orc = new Orc2Hfile();
        orc.setConf(conf);
        ControlledJob orcJob = orc.getControlledJob();
        jobc.addJob(orcJob);

        JobRunResult result = JobRunUtil.run(jobc);
        result.print(true);//
        // ------下面的是执行导入hbase表的代码--------------------------
//		注意： 下面的代码，在本地是执行不了的，需要在集群上运行，才可以执行
//		hadoop jar /usr/local/hbase/lib/hbase-shell-1.3.1.jar completebulkload
//		/user/hadoop/hbase/output/orc2hfile_0412 user_install_status
//        String outputPath = orc.getOutputPath(orc.getJobNameWithTaskId()).toString();
//        String hbaseTableName = Orc2Hfile.tableName.toString();
//
//        String[] params = {outputPath, hbaseTableName};
//        LoadIncrementalHFiles.main(params);

        return 0;
    }

    public static void main (String [] args ) throws Exception {

        //-Dtask.input.dir=/Users/shuyang/file/input -Dtask.id=0415 -Dtask.base.dir=/Users/shuyang/file/output
        System.exit(ToolRunner.run(new Orc2HfileJob(),args));
    }

}
