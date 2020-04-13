/**
 * WordCount.java
 * com.hainiuxy.mapreduce
 * Copyright (c) 2019, 海牛版权所有.
 * @author   潘牛                      
*/

package com.hainiuxy.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 带有压缩的wordcount 程序
 * @author   潘牛                      
 * @Date	 2019年3月25日 	 
 */
public class WordCountWithCompress extends Configured implements Tool{
	/*
	 * public class TextInputFormat extends FileInputFormat<LongWritable, Text>
	 * 
	 * TextInputFormat 提供了 createRecordReader（）， 返回linerecordreader 实例对象
	 * public class LineRecordReader extends RecordReader<LongWritable, Text> 
	 * 
	 * keyin:LongWritable
	 * valueIn: Text
	 * 
	 */
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		Text keyOut = new Text();
		
		LongWritable valueOut = new LongWritable(1L);
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 统计输入数据的行数
			context.getCounter("hainiu_class11", "line count num").increment(1L);
			
			String flag = context.getConfiguration().get("mapreduce.output.fileoutputformat.compress");
			System.out.println("print flag:" + flag);
			System.out.println("------map()---------------------");
			// aa bb cc  ---> [aa,bb,cc]
			String[] splits = value.toString().split(" ");
			for(String w : splits){
				keyOut.set(w);
				//aa , 1
				//bb , 1
				//cc , 1
				context.write(keyOut, valueOut);
				System.out.println("map output:==>" + w + ", " + valueOut.get());
			}
			
		}
		
		
	}
	
	
	public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		LongWritable valueOut = new LongWritable();
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			System.out.println("-----reduce()----------------");
			// 统计一共有多少种key
			context.getCounter("hainiu_class11", "key type num").increment(1L);
			// aa, [1,1,1]  ----> aa, 3
			long sum = 0L;
			StringBuilder sb = new StringBuilder("reduce input==>key:" + key.toString() + ", values:[");
			for(LongWritable w : values){
				long num = w.get();
				sb.append(num).append(",");
				sum += num;
			}
			
			sb.deleteCharAt(sb.length() - 1).append("]");
			System.out.println(sb.toString());
			
			valueOut.set(sum);
			System.out.println("reduce output==> " + key.toString() + ", " + valueOut.get());
			context.write(key, valueOut);
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		// 获取configuration对象
		Configuration conf = getConf();
		
//		conf.set(FileOutputFormat.COMPRESS, "true");
//		
//		conf.set(FileOutputFormat.COMPRESS_CODEC, GzipCodec.class.getName());
//		
		// 设置map输出压缩
		conf.set(MRJobConfig.MAP_OUTPUT_COMPRESS, "false");
		conf.set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, SnappyCodec.class.getName());
		
		
		// 创建job对象
		Job job = Job.getInstance(conf, "wordcount");
		
		// job.setjarby
		job.setJarByClass(WordCountWithCompress.class);
		
		// job.setmapclass
		job.setMapperClass(WordCountMapper.class);
		
		// job.setmapoutputkeyclass
		job.setMapOutputKeyClass(Text.class);
		
		//job.setmapoutputvalueclass
		job.setMapOutputValueClass(LongWritable.class);
		
		// job.setreduceclass
		job.setReducerClass(WordCountReducer.class);
		
		// job.setoutputkeyclass
		job.setOutputKeyClass(Text.class);
		// job.setoutputvalueclass
		job.setOutputValueClass(LongWritable.class);
		// 设置reduce个数
		job.setNumReduceTasks(1);
		
		// job.setinputformatclass
		job.setInputFormatClass(TextInputFormat.class);
		
		// job.setoutputformat.class
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// 设置 输入目录
		FileInputFormat.addInputPath(job, new Path("test/wordCount/input/"));
		
//		// 设置开启reduce输出压缩
		FileOutputFormat.setCompressOutput(job, true);
		// 设置输出文件的压缩格式
		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		
/*		job.getConfiguration().set(FileOutputFormat.COMPRESS, "true");
		
		job.getConfiguration().set(FileOutputFormat.COMPRESS_CODEC, GzipCodec.class.getName());*/
		// 设置输出目录
		Path outputDir = new Path("test/wordCount/output/");
		FileOutputFormat.setOutputPath(job, outputDir);
		
		// 自动删除输出目录
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputDir)){
			fs.delete(outputDir, true);
			System.out.println("delete output dir: " + outputDir.toString() + " success!");
		}
		
		// 提交job任务
		boolean status = job.waitForCompletion(true);
		
		Counters counters = job.getCounters();
		
		CounterGroup group = counters.getGroup("hainiu_class11");
		System.out.println("hainiu_class11");
		for(Counter counter : group){
			System.out.println("\t" + counter.getDisplayName() + "=" + counter.getValue());
			
		}
		
		System.out.println("-----查找指定counter--------------");
		Counter c = group.findCounter("line count num");
		System.out.println(c.getDisplayName() + "=" + c.getValue());
		
		return status ? 0 : 1;
		
	}
	
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCountWithCompress(), args));
	}

}

