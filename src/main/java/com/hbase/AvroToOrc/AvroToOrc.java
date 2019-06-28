package com.hbase.AvroToOrc;

import com.teacher.base.BaseMR;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroToOrc extends BaseMR {



    /**
     * keyin valuein: 参考avrokeyinputformat
     *
     * keyout valueout ： 参考orcnewoutputformat
     */

    public static class AvroToOrcMapper extends Mapper<AvroKey<GenericRecord>, NullWritable , NullWritable, Writable> {

        /**
         * 写 orc 的 inspector 对象
         */
        StructObjectInspector inspector = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String type = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>";
            TypeInfo info = TypeInfoUtils.getTypeInfoFromTypeString(type);
            inspector = (StructObjectInspector)TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(info);
        }

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            GenericRecord genericRecord = key.datum();
            /**
             * 取值阶段 start
             */
            String aid = genericRecord.get("aid").toString();
            String pkgname = genericRecord.get("pkgname").toString();
            long uptime = (long)genericRecord.get("uptime");
            int type = (int)genericRecord.get("type");
            String country = genericRecord.get("country").toString();
            /**
             * 取值阶段 end
             */
            /**
             * 写orc文件 start
             */
            OrcSerde orcSerde = new OrcSerde();
            List rowList = new ArrayList();
            rowList.add(aid);
            rowList.add(pkgname);
            rowList.add(uptime);
            rowList.add(type);
            rowList.add(country);
            // 通过写orc文件的inspector对象，将数据序列化为orc格式的writable
            Writable writable = orcSerde.serialize(rowList , inspector);
            /**
             * 写 orc 文件 end
             */
            context.write(NullWritable.get() , writable);

        }
    }


    @Override
    public Job getJob() throws IOException {
        Job job = Job.getInstance(conf , getJobNameWithTaskId());

        job.setJarByClass(AvroToOrc.class);
        job.setMapperClass(AvroToOrcMapper.class);

        job.setNumReduceTasks(0);

        // 输入类型
        job.setInputFormatClass(AvroKeyInputFormat.class);

        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(Writable.class);

        job.setOutputFormatClass(OrcNewOutputFormat.class);


        Schema schema = null;
        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(new FileInputStream(new File("config/user_install_status_avro.avsc")));

        AvroJob.setInputKeySchema(job , schema);
        FileInputFormat.addInputPath(job , getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job , getOutputPath(getJobNameWithTaskId()));

        return job;
    }

    @Override
    public String getJobName() {
        return "AvroToOrc";
    }
}
