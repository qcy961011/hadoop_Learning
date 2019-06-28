/**
 * Orc2Hfile.java
 * com.hainiuxy.mapreduce.mrrun.hbase
 * Copyright (c) 2019, 海牛版权所有.
 * @author   潘牛                      
*/

package com.teacher.hbase;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.teacher.base.BaseMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 读取orc文件，生成hfile文件
 * @author   潘牛                      
 * @Date	 2019年4月12日 	 
 */
public class Orc2Hfile extends BaseMR {
	
	public static TableName tableName = TableName.valueOf("user_install_status");
	
	/*
	 * 参考TextInputformat, orc 文件 有 OrcNewInputFormat
	 * public class OrcNewInputFormat extends InputFormat<NullWritable, OrcStruct>
	 * 		重写方法createRecordReader 里面 返回了 OrcRecordReader
	 * 
	 * class OrcRecordReader extends RecordReader<NullWritable, OrcStruct>
	 * keyin: NullWritable
	 * valuein: OrcStruct
	 * --------------------
	 * 要想生成hfile文件， 需要用 HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), table.getRegionLocator());
	 * 提供的配置方法， 在这方法里面
	 * 可以确定reduce --》 PutSortReducer
	 * 又 因为 PutSortReducer extends Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, KeyValue>
	 * 
	 * reduce的输入类型是map的输出类型
	 * keyOut: ImmutableBytesWritable --> 封装的byte[]  --> 放的是表的rowkey
	 * 
	 * valueOut: Put, put 可以put一行的数据进去
	 * 
	 * 
	 */
	public static class Orc2HfileMapper extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put>{
		
		/**
		 * 读取orc文件的inspector对象
		 */
		StructObjectInspector inspector = null;
		
		ImmutableBytesWritable keyOut = new ImmutableBytesWritable();
		
		SimpleDateFormat dfs = new SimpleDateFormat("yyyyMMdd");
		
		/*
		 * 初始化orc
		 */
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// 通过 hive --orcfiledump orc文件对应的hdfs目录，查看 orc文件的信息，看type
			String type = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>";
			// 根据type类型创建指定的typeinfo对象
			TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
			// 再根据typeinfo 对象创建相应类型的inspector
			inspector = (StructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
		}
		
		/*
		 * 解析orc
		 */
		@Override
		protected void map(NullWritable key, OrcStruct orcObj,Context context)
				throws IOException, InterruptedException {
//			aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string
			
			String aid = getStructData(orcObj, "aid");
			
			String pkgname = getStructData(orcObj, "pkgname");
			// 1970年到指定时间的秒数
			String uptimestr = getStructData(orcObj, "uptime");
			String typestr = getStructData(orcObj, "type");
			String country = getStructData(orcObj, "country");
			String gpcategory = getStructData(orcObj, "gpcategory");
			
//			System.out.println("----------------------------");
//			System.out.println("aid         :" +  aid           );
//			System.out.println("pkgname     :" +  pkgname       );
//			System.out.println("uptime      :" +  uptimestr        );
//			System.out.println("type        :" +  typestr          );
//			System.out.println("country     :" +  country       );
//			System.out.println("gpcategory  :" +  gpcategory    );
			
			
			// rowkey : aid_yyyyMMdd
			// 通过毫秒数创建date对象
			Date date = new Date(Long.parseLong(uptimestr) * 1000);
			String formatUptime = dfs.format(date);
			
			String rowkey = aid + "_" + formatUptime;
			
			keyOut.set(toBytes(rowkey));
			
			// 一行的数据
			Put put = new Put(toBytes(rowkey));
			
			
			if(pkgname != null){
				put.addColumn(toBytes("cf"), toBytes("pkgname"), toBytes(pkgname));
			}
			
			if(formatUptime != null){
				put.addColumn(toBytes("cf"), toBytes("uptime"), toBytes(uptimestr));
			}
			
			if(typestr != null){
				put.addColumn(toBytes("cf"), toBytes("type"), toBytes(typestr));
			}
			
			if(country != null){
				put.addColumn(toBytes("cf"), toBytes("country"), toBytes(country));
			}
			
			if(gpcategory != null){
				put.addColumn(toBytes("cf"), toBytes("gpcategory"), toBytes(gpcategory));
			}
			
			
			context.write(keyOut, put);
			
		}
		
		private String getStructData(OrcStruct orcObj, String fieldName){
			// 根据字段名称获取struct 里面的 字段对象
			StructField structFieldRef = inspector.getStructFieldRef(fieldName);
			// 根据字段对象去 orcStruct 里面找到数据
			String data = String.valueOf(inspector.getStructFieldData(orcObj, structFieldRef));
			
			data = (data == null || "null".equalsIgnoreCase(data)) ? null : data;
			
			return data;
		}
		
	}

	@Override
	public Job getJob() throws IOException {
		Job job = Job.getInstance(conf, getJobNameWithTaskId());
		// job.setjarby
		job.setJarByClass(Orc2Hfile.class);
		
		// job.setmapclass
		job.setMapperClass(Orc2HfileMapper.class);
		
		// job.setmapoutputkeyclass
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		
		//job.setmapoutputvalueclass
		job.setMapOutputValueClass(Put.class);
		

		// 设置reduce个数
		job.setNumReduceTasks(0);
		
		// job.setinputformatclass
		job.setInputFormatClass(OrcNewInputFormat.class);
		
		
		// 这几行代码都是围绕着下面的代码写的
		Configuration hbaseConf = HBaseConfiguration.addHbaseResources(conf);
		
		Connection conn = ConnectionFactory.createConnection(hbaseConf);
		
		HTable table = (HTable)conn.getTable(tableName);
		
		// 写hfile文件的job任务配置。
		HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), table.getRegionLocator());
		
		FileInputFormat.addInputPath(job, getFirstJobInputPath());
		
		// 设置输出目录
		Path outputDir = getOutputPath(getJobNameWithTaskId());
		FileOutputFormat.setOutputPath(job, outputDir);
		
		return job;
		
	}

	@Override
	public String getJobName() {
		
		return "orc2hfile";
		
	}

}

