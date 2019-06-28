package com.hbase.HbaseFileGenerate;

import com.qiao.test.baseMr.BaseMR;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.hbase.util.Bytes;
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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FileGenerate extends BaseMR {

    /**
     * 使用OrcNewInputFormat.class
     * 泛型参考以上类
     */

    private static class FileGenerateMapper extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put> {

        Log log = LogFactory.getLog(FileGenerateMapper.class);

        /**
         * 多去orc文件的inspector对象
         */
        StructObjectInspector structObjectInspector = null;

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

        ImmutableBytesWritable keyOut = new ImmutableBytesWritable();

        @Override
        protected void setup(Context context) {
            log.info("====================  Mapper setup  ====================");
            // 数据对应类型的json拼串
            String type = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>";
            TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
            structObjectInspector = (StructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
        }

        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            log.info("====================  Mapper map  ====================");
            String aid = getStructData(value, "aid");
            String pkgname = getStructData(value, "pkgname");
            String uptimestr = getStructData(value, "uptime");
            String type = getStructData(value, "type");
            String country = getStructData(value, "country");
            String gpcategory = getStructData(value, "gpcategory");

            log.info(aid);
            log.info(pkgname);
            log.info(uptimestr);
            log.info(type);
            log.info(country);
            log.info(gpcategory);


            Date date = new Date(Long.parseLong(uptimestr) * 1000);
            String format = simpleDateFormat.format(date);
            String rowKey = aid + "_" + format;
            keyOut.set(Bytes.toBytes(rowKey));
            Put put = new Put(Bytes.toBytes(rowKey));
            if (pkgname != null) {
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pkgname"), Bytes.toBytes(pkgname));
            }

            if (format != null) {
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("uptime"), Bytes.toBytes(uptimestr));
            }

            if (type != null) {
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("type"), Bytes.toBytes(type));
            }

            if (country != null) {
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("country"), Bytes.toBytes(country));
            }

            if (gpcategory != null) {
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("gpcategory"), Bytes.toBytes(gpcategory));
            }

            context.write(keyOut, put);
        }

        private String getStructData(OrcStruct orcStruct, String fildName) {
            StructField structField = structObjectInspector.getStructFieldRef(fildName);

            String string = structObjectInspector.getStructFieldData(orcStruct, structField).toString();

            string = (string == null || "null".equalsIgnoreCase(string)) ? null : string;
            return string;
        }


    }


    @Override
    public Job getJob() throws IOException {
        System.out.println("生成Job");
        Job job = Job.getInstance(getConfiguration(), getJobNameWithTaskId());

        job.setJarByClass(FileGenerate.class);

        job.setMapperClass(FileGenerateMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setInputFormatClass(OrcNewInputFormat.class);
        job.setNumReduceTasks(0);

        Configuration hbaseConfiguration = HBaseConfiguration.addHbaseResources(getConfiguration());

        Connection conn = ConnectionFactory.createConnection(hbaseConfiguration);

        TableName tableName = TableName.valueOf("qiaochunyu:user_install_status");
        HTable hTable = (HTable) conn.getTable(tableName);

        HFileOutputFormat2.configureIncrementalLoad(job, hTable.getTableDescriptor(), hTable.getRegionLocator());
        FileInputFormat.addInputPath(job, getFirstJobInputPath());
        Path outputPath = getOutputPath(getJobNameWithTaskId());
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    @Override
    public String getJobName() {
        return "FileGenerate";
    }
}
