package com.qiao.test.masterKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MasterKeyDiver {

    private static class MasterKeyMapper extends Mapper<LongWritable, Text, MasterKeyWritable, Text> {
        MasterKeyWritable keyOut = new MasterKeyWritable();

        Text valueOut = new Text();
        @Override
        protected void map(LongWritable key, Text value,Context context)
                throws IOException, InterruptedException {
            String[] splits = value.toString().split(" ");
            String word = splits[0];
            long num = Long.parseLong(splits[1]);


            keyOut.setSlaveKey(new Text(word));
            keyOut.setMatserKey(new LongWritable(num));

            valueOut.set(word + "##" + num);
            context.write(keyOut, valueOut);


        }
    }

    private static class MasterKeyReduce extends Reducer<MasterKeyWritable, Text, Text, LongWritable> {
        Text keyOut = new Text();

        LongWritable valueOut = new LongWritable();

        @Override
        protected void reduce(MasterKeyWritable key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder("reduce input==>key:" + key.toString() + ", values:[");
            for(Text t : values){
                sb.append(t.toString()).append(",");
                String[] splits = t.toString().split("##");
                keyOut.set(splits[0]);
                valueOut.set(Long.parseLong(splits[1]));
                context.write(keyOut, valueOut);
            }

            sb.deleteCharAt(sb.length() - 1).append("]");
            System.out.println(sb.toString());

        }
    }

    /**
     * 在Reduce阶段有个排序分组的过程
     * 默认是按照主关键字和次关键字组合来分组
     * 如果想按照主关键字来分组，需要自定义分组排序的外部比较器来分组
     * 如果分完组调用reduce的次数会减少
     * 有多少组，就调用多少次
     */

    private static class MasterKeyGroup extends WritableComparator {
        public MasterKeyGroup(){
            super(MasterKeyWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            System.out.println("---group sort----compare----------------");
            MasterKeyWritable wa = (MasterKeyWritable) a;
            MasterKeyWritable wb = (MasterKeyWritable) b;
            return wa.getSlaveKey().compareTo(wb.getSlaveKey());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration , "masterKey");
        job.setJarByClass(MasterKeyDiver.class);
        job.setMapperClass(MasterKeyMapper.class);
        job.setReducerClass(MasterKeyReduce.class);

        job.setMapOutputKeyClass(MasterKeyWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        /** 配置分组 */
        job.setGroupingComparatorClass(MasterKeyGroup.class);


        FileInputFormat.setInputPaths(job , new Path("test/materKey/input"));
        Path output = new Path("test/materKey/output");
        FileOutputFormat.setOutputPath(job , output);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(output)) {
            fs.delete(output , true);
        }
        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }


}
