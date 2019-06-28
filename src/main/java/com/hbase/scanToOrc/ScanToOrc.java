package com.hbase.scanToOrc;

import com.util.base.BaseMR;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.orc.CompressionKind;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ScanToOrc extends BaseMR {

    public static TableName tableName = TableName.valueOf("user_install_status_limit");

    public static class ScanToOrcMapper extends TableMapper<NullWritable, Writable> {
        StructObjectInspector inspector = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String type = "struct<aid:string,pkgname:string,uptime:bigint,type:int,country:string,gpcategory:string>";
            TypeInfo info = TypeInfoUtils.getTypeInfoFromTypeString(type);
            inspector = (StructObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(info);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String rowkey = Bytes.toString(key.get());
            String aid = rowkey.split("_")[0];
            String pkgname = null, country = null, gpcategory = null;
            long uptime = -1L;
            int type = -1;
            Cell[] rawCells = value.rawCells();
            for (Cell cell :
                    rawCells) {
                String cName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String cValue = Bytes.toString(CellUtil.cloneValue(cell));
                switch (cName) {
                    case "pkgname":
                        pkgname = cValue;
                        break;
                    case "uptime":
                        uptime = Long.parseLong(cValue);
                        break;
                    case "type":
                        type = Integer.parseInt(cValue);
                        break;
                    case "country":
                        country = cValue;
                        break;
                    case "gpcategory":
                        gpcategory = cValue;
                        break;
                    default:

                }
            }

            OrcSerde serde = new OrcSerde();
            List realRow = new ArrayList();
            realRow.add(aid);
            realRow.add(pkgname);
            realRow.add(uptime);
            realRow.add(type);
            realRow.add(country);
            realRow.add(gpcategory);
            Writable w = serde.serialize(realRow , inspector);
            context.write(NullWritable.get() , w);
        }
    }


    @Override
    public Job getJob() throws IOException {

        conf.set("mapreduce.map.sprdulative", "false");

        conf.set("orc.compress", CompressionKind.SNAPPY.name());

        conf.set("orc.create.index", "true");

        Job job = Job.getInstance(conf, getJobNameWithTaskId());

        job.setJarByClass(ScanToOrc.class);

        job.setMapperClass(ScanToOrcMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Writable.class);

        job.setNumReduceTasks(0);
        job.setOutputFormatClass(OrcNewOutputFormat.class);

        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob(tableName, scan, ScanToOrcMapper.class, NullWritable.class, Writable.class, job);

        // 设置输出目录
        Path outputDir = getOutputPath(getJobNameWithTaskId());
        FileOutputFormat.setOutputPath(job, outputDir);
        return job;

    }

    @Override
    public String getJobName() {
        return "ScanToOrc";
    }
}
