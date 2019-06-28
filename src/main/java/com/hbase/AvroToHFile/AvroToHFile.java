package com.hbase.AvroToHFile;

import com.teacher.base.BaseMR;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AvroToHFile extends BaseMR {

    private static class AvroToHFileMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, ImmutableBytesWritable, Put> {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

        ImmutableBytesWritable keyOut = new ImmutableBytesWritable();
        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            GenericRecord genericRecord = key.datum();
            String aid = genericRecord.get("aid").toString();
            String pkgname = genericRecord.get("pkgname").toString();
            String uptime = genericRecord.get("uptime").toString();
            String type = genericRecord.get("type").toString();
            String country = genericRecord.get("country").toString();
            String gpcategory = genericRecord.get("gpcategory").toString();


            Date date = new Date(Long.parseLong(uptime) * 1000);
            String format = simpleDateFormat.format(date);
            String rowKey = aid + "_" + format;

            keyOut.set(Bytes.toBytes(rowKey));
            Put put = new Put(Bytes.toBytes(rowKey));
            if (pkgname != null) {
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("pkgname"), Bytes.toBytes(pkgname));
            }

            if (format != null) {
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("uptime"), Bytes.toBytes(uptime));
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


    }

    @Override
    public Job getJob() throws IOException {
        Job job = Job.getInstance(conf , getJobNameWithTaskId());

        job.setJarByClass(AvroToHFile.class);
        job.setMapperClass(AvroToHFileMapper.class);

        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setInputFormatClass(AvroKeyInputFormat.class);


        Configuration hbaseConfiguration = HBaseConfiguration.addHbaseResources(conf);
        Connection conn = ConnectionFactory.createConnection(hbaseConfiguration);

        TableName tableName = TableName.valueOf("qiaochunyu:user_0415");
        HTable hTable = (HTable) conn.getTable(tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, hTable.getTableDescriptor(), hTable.getRegionLocator());
        Schema schema = null;
        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(AvroToHFile.class.getResourceAsStream("/user_install_status_avro.avsc"));

        AvroJob.setInputKeySchema(job , schema);
        FileInputFormat.addInputPath(job , getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job , getOutputPath(getJobNameWithTaskId()));
        return job;
    }

    @Override
    public String getJobName() {
        return "AvroToHFile";
    }
}
