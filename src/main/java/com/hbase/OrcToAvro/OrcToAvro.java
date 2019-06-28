package com.hbase.OrcToAvro;


import com.teacher.base.BaseMR;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
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

import java.io.*;

public class OrcToAvro extends BaseMR {

    public static Schema schema = null;

    public static Schema.Parser parser = new Schema.Parser();

    /**
     * class AvroKeyOutputFormat<T> extends AvroOutputFormatBase<AvroKey<T>, NullWritable>
     * 又因为： getRecordWriter方法 返回 AvroKeyRecordWriter 实例
     * class AvroKeyRecordWriter<T> extends RecordWriter<AvroKey<T>, NullWritable>
     * keyout: AvroKey
     * valueout: NullWritable
     *
     */
    public static class OrcToAvroMapper extends Mapper<NullWritable, OrcStruct , AvroKey<GenericRecord>, NullWritable > {
        /**
         * 读orc文件的inspector
         */
        StructObjectInspector inspector = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String type = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>";
            TypeInfo info = TypeInfoUtils.getTypeInfoFromTypeString(type);
            inspector = (StructObjectInspector) OrcStruct.createObjectInspector(info);
            // 重新解析一次，如果schema对象不是空的，就不需要重复解析，否则报错
            if(schema == null){
                schema = parser.parse(new FileInputStream(new File("config/user_install_status_avro.avsc")));
            }
        }

        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            /**
             *  获取orc文件数据 start
             */
            String aid = getStructData(value, "aid");

            String pkgname = getStructData(value, "pkgname");
            // 1970年到指定时间的秒数
            String uptimestr = getStructData(value, "uptime");
            String typestr = getStructData(value, "type");
            String country = getStructData(value, "country");

            /**
             *  获取orc文件数据 end
             */
            //根据创建的Schema对象，创建一行的对象
            GenericRecord record = new GenericData.Record(schema);
            record.put("aid", aid);
            record.put("pkgname", pkgname);
            record.put("uptime", Long.parseLong(uptimestr));
            record.put("type", Integer.parseInt(typestr));
            record.put("country", country);
            // 将一行的数据封装到avrokey对象中
            context.write(new AvroKey<GenericRecord>(record), NullWritable.get());
        }

        private String getStructData(OrcStruct orcStruct, String fildName) {
            StructField structField = inspector.getStructFieldRef(fildName);

            String string = inspector.getStructFieldData(orcStruct, structField).toString();

            string = (string == null || "null".equalsIgnoreCase(string)) ? null : string;
            return string;
        }
    }



    @Override
    public Job getJob() throws IOException {
        Job job = Job.getInstance(conf , getJobNameWithTaskId());

        job.setJarByClass(OrcToAvro.class);
        job.setMapperClass(OrcToAvroMapper.class);

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setInputFormatClass(OrcNewInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        schema = parser.parse(new FileInputStream(new File("config/user_install_status_avro.avsc")));

        AvroJob.setMapOutputKeySchema(job , schema);

        FileInputFormat.addInputPath(job , getFirstJobInputPath());
        // 设置输出目录
        FileOutputFormat.setOutputPath(job, getOutputPath(getJobNameWithTaskId()));

        return job;
    }

    @Override
    public String getJobName() {
        return "OrcToAvro";
    }
}
