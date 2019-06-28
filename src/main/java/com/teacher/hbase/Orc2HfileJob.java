/**
 * WordMaxJob.java
 * com.hainiuxy.mapreduce.mrrun
 * Copyright (c) 2019, 海牛版权所有.
 * @author   潘牛                      
*/

package com.teacher.hbase;

import com.teacher.util.JobRunResult;
import com.teacher.util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * orc2hfile job
 * @author   潘牛                      
 * @Date	 2019年3月29日 	 
 */
public class Orc2HfileJob extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
//		1）获取配置对象
		Configuration conf = getConf();
//		2）创建任务链对象 JobControl
		JobControl jobc = new JobControl("Orc2HfileJob");
		
//		3）创建任务链中要添加的任务对象  ControlledJob
		Orc2Hfile orc = new Orc2Hfile();
		orc.setConf(conf);
		
		ControlledJob orcCJob = orc.getControlledJob();
		
	
//		5）将任务添加到任务链中 
		jobc.addJob(orcCJob);
		
		JobRunResult result = JobRunUtil.run(jobc);
		result.print(true);

//		------下面的是执行导入hbase表的代码--------------------------
//		注意： 下面的代码，在本地是执行不了的，需要在集群上运行，才可以执行
//		hadoop jar /usr/local/hbase/lib/hbase-shell-1.3.1.jar completebulkload 
//		/user/hadoop/hbase/output/orc2hfile_0412 user_install_status
//		String outputPath = orc.getOutputPath(orc.getJobNameWithTaskId()).toString();
//		String hbaseTableName = Orc2Hfile.tableName.toString();
//
//		String[] params = {outputPath, hbaseTableName};
//		// 参考 hbase导入命令
//		LoadIncrementalHFiles.main(params);
		
		return 0;
		
	}

	
	public static void main(String[] args) throws Exception {
//		-Dtask.id=0412 -Dtask.input.dir=data/input -Dtask.base.dir=data/output -Dhbase.zookeeper.quorum=nn1.hadoop:2181,nn2.hadoop:2181,s1.hadoop:2181
		System.exit(ToolRunner.run(new Orc2HfileJob(), args));
	}

}

