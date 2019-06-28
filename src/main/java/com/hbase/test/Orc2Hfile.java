package com.hbase.test;

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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class Orc2Hfile extends BaseMR {
    //

    public static TableName tableName = TableName.valueOf("user_install_status");


    public static class Orc2HfileMapper
            extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put>{
        //读取orc文件的inspector对象
        StructObjectInspector inspector=null;

        SimpleDateFormat dfs = new SimpleDateFormat("yyyyMMdd");

        ImmutableBytesWritable keyOut= new ImmutableBytesWritable();
        @Override
        public void setup(Context context){
            String type="struct<aid:string,pkname:string,uptime:bigint,type:int,country:string,gpcategory:string>";
            TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(type);
            inspector= (StructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
        }

        @Override
        public void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            System.out.println("test");
            String aid = getStructData(value, "aid");
            String uptimestr = getStructData(value, "uptime");
            String typestr = getStructData(value, "type");
            String country = getStructData(value, "country");
            String gpcategory = getStructData(value, "gpcategory");

            System.out.println("----------------------------");
            System.out.println("aid         :" +  aid        );
            System.out.println("uptime      :" +  uptimestr  );
            System.out.println("type        :" +  typestr    );
            System.out.println("country     :" +  country    );
            System.out.println("gpcategory  :" +  gpcategory );

            Date date = new Date(Long.parseLong(uptimestr)*1000);

            String formatUptime = dfs.format(date);
            String rowKey = aid+"_"+formatUptime;
            Put put = new Put(toBytes(rowKey));

            keyOut.set(toBytes(rowKey));

//            if(pkgname !=null){
//                put.addColumn(toBytes("cf"),toBytes("pkgname"),toBytes(pkgname));
//
//
//            }

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
        private String getStructData(OrcStruct value,String key){
            StructField structFieldRef=inspector.getStructFieldRef(key);
            String data = String.valueOf(inspector.getStructFieldData(value,structFieldRef));
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
        // 设置输出目录
        Configuration hbaseConf = HBaseConfiguration.addHbaseResources(conf);

        Connection conn = ConnectionFactory.createConnection(hbaseConf);

        HTable table = (HTable)conn.getTable(tableName);

        HFileOutputFormat2.configureIncrementalLoad(job, table.getTableDescriptor(), table.getRegionLocator());


        FileInputFormat.addInputPath(job, getFirstJobInputPath());
        Path outputDir = getOutputPath(getJobNameWithTaskId());

        FileOutputFormat.setOutputPath(job,outputDir);
        return job;
    }
    //read orc file from hdfs
    @Override
    public String getJobName() {
        return "orc2file";
    }

}
