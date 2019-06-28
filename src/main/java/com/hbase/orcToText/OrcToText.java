package com.hbase.orcToText;

import com.hbase.until.OrcUtil;
import com.teacher.base.BaseMR;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class OrcToText extends BaseMR {


    private static class OrcToTextMapper extends Mapper<NullWritable, OrcStruct, Text , Text> {

        OrcUtil orcUtil = new OrcUtil();
        @Override
        protected void setup(Context context) {
            orcUtil.setOrcTypeReadSchema();
        }

        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            /**
             * 获取orc文件数据 start
             */
            String aid = orcUtil.getOrcData(value , "aid");
            String pkgname = orcUtil.getOrcData(value, "pkgname");
            // 1970年到指定时间的秒数
            String uptimestr = orcUtil.getOrcData(value, "uptime");
            String typestr = orcUtil.getOrcData(value, "type");
            String country = orcUtil.getOrcData(value, "country");
            String gpcategory = orcUtil.getOrcData(value, "gpcategory");
            System.out.println(aid);
            System.out.println(pkgname);
            System.out.println(uptimestr);
            System.out.println(typestr);
            System.out.println(country);
            System.out.println(gpcategory);
            System.out.println("================");
            context.write(new Text(aid) , new Text(pkgname + "\t" + uptimestr + "\t" + typestr + "\t" + country));

        }

    }




    @Override
    public Job getJob() throws IOException {
        Job job = Job.getInstance(conf , getJobNameWithTaskId());

        job.setJarByClass(OrcToText.class);

        job.setMapperClass(OrcToTextMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(OrcNewInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job , getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job , getOutputPath(getJobNameWithTaskId()));

        return job;
    }

    @Override
    public String getJobName() {
        return "OrcToText";
    }
}
