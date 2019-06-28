package com.hbase.AvroToText;

import com.teacher.base.BaseMR;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class AvroToText extends BaseMR {

    private static class AvtoToTextMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text , Text> {

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            GenericRecord genericRecord = key.datum();
            String aid = genericRecord.get("aid").toString();
            String pkgname = genericRecord.get("pkgname").toString();
            long uptime = (long)genericRecord.get("uptime");
            int type = (int)genericRecord.get("type");
            String country = genericRecord.get("country").toString();
            context.write(new Text(aid) , new Text(pkgname + "\t" + uptime + "\t" + type + "\t" + country));
        }
    }


    @Override
    public Job getJob() throws IOException {
        Job job = Job.getInstance(conf , getJobNameWithTaskId());
        job.setJarByClass(AvroToText.class);
        job.setMapperClass(AvtoToTextMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(AvroKeyInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(AvroToText.class.getResourceAsStream("/log_schema.txt"));

        AvroJob.setInputKeySchema(job , schema);
        FileInputFormat.addInputPath(job , getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job , getOutputPath(getJobNameWithTaskId()));
        return job;
    }

    @Override
    public String getJobName() {
        return "AvroToText";
    }
}
