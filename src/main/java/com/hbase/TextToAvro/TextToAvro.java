package com.hbase.TextToAvro;

import com.teacher.base.BaseMR;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class TextToAvro extends BaseMR {

    private static class TextToAvroMapper extends Mapper{

    }


    @Override
    public Job getJob() throws IOException {
        Job job = Job.getInstance(conf , getJobNameWithTaskId());
        job.setJarByClass(TextToAvro.class);
        job.setMapperClass(TextToAvroMapper.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(new FileInputStream(new File("config/user_install_status_avro.avsc")));



        return null;
    }

    @Override
    public String getJobName() {
        return "TextToAvro";
    }
}
