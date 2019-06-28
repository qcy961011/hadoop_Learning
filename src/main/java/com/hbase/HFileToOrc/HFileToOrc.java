package com.hbase.HFileToOrc;


import com.hbase.until.CellItemWritable;
import com.hbase.until.HFileInputFormat;
import com.hbase.until.OrcUtil;
import com.util.base.BaseMR;
import groovy.json.internal.Byt;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.orc.CompressionKind;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HFileToOrc extends BaseMR {

    private static class HFileToOrcMapper extends Mapper<ImmutableBytesWritable, Cell, ImmutableBytesWritable, CellItemWritable>{
        @Override
        protected void map(ImmutableBytesWritable key, Cell value, Context context) throws IOException, InterruptedException {
            String rowkey = Bytes.toString(key.get());
            String aid = rowkey.split("_")[0];
            // 字段名称
            String cName = Bytes.toString(CellUtil.cloneQualifier(value));
            // 字段值
            String cValue = Bytes.toString(CellUtil.cloneValue(value));
            // 时间戳
            long timestamp = value.getTimestamp();
            // 最新的列是否被删除
            boolean deleted = CellUtil.isDelete(value);
            System.out.println("aid:" + aid + "| " + cName + ":" + cValue + "| timestamp:" + timestamp + "| isdeleted:" + deleted);
            CellItemWritable item = new CellItemWritable(cName, cValue, timestamp, deleted);
            context.write(key, item);
        }
    }



    public static class HFileToOrcReducer extends Reducer<ImmutableBytesWritable, CellItemWritable, NullWritable, Writable> {

        OrcUtil orcUtil = new OrcUtil();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            orcUtil.setOrcTypeWriteSchema();

        }

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<CellItemWritable> values,
                              Reducer<ImmutableBytesWritable, CellItemWritable, NullWritable, Writable>.Context context)
                throws IOException, InterruptedException {

            String rowkey = Bytes.toString(key.get());
            String aid = rowkey.split("_")[0];

//			rowkey, ['pkgname 的数据1'， 'pkgname 的数据2'， 'country 的数据1']
            List<CellItemWritable> pkgnames = new ArrayList<CellItemWritable>();
            List<CellItemWritable> uptimes = new ArrayList<CellItemWritable>();
            List<CellItemWritable> types = new ArrayList<CellItemWritable>();
            List<CellItemWritable> countrys = new ArrayList<CellItemWritable>();
            List<CellItemWritable> gpcategorys = new ArrayList<CellItemWritable>();

            for (CellItemWritable v : values) {
                // 重新创建对象，把数据copy到新的对象里,为了避免reduce Iterable的坑
                CellItemWritable item = new CellItemWritable(v.getName(), v.getValue(), v.getTimestamp(), v.isDeleted());
                // 根据字段名称把cell 装到指定的list里面，为了对每个字段列表排序，找到最新版本的
                switch (item.getName()) {

                    case "pkgname":
                        pkgnames.add(item);
                        break;
                    case "uptime":
                        uptimes.add(item);
                        break;
                    case "type":
                        types.add(item);
                        break;
                    case "country":
                        countrys.add(item);
                        break;
                    case "gpcategory":
                        gpcategorys.add(item);
                        break;
                    default:

                }
            }

            // 排序
            Collections.sort(pkgnames);
            Collections.sort(uptimes);
            Collections.sort(types);
            Collections.sort(countrys);
            Collections.sort(gpcategorys);

            // 对每个字段找出最大版本的数据，如果是已删除的就跳过
            CellItemWritable pkgnameNewer = getNewerTimestampData(pkgnames);
            CellItemWritable uptimeNewer = getNewerTimestampData(uptimes);
            CellItemWritable typeNewer = getNewerTimestampData(types);
            CellItemWritable countryNewer = getNewerTimestampData(countrys);
            CellItemWritable gpcategoryNewer = getNewerTimestampData(gpcategorys);

            // 如果所有的字段都是初始版本，timestamp = 0； 说明没有新版本，这一行就不要了，不写入orc文件
            if (pkgnameNewer.getTimestamp() == 0 && uptimeNewer.getTimestamp() == 0 &&
                    typeNewer.getTimestamp() == 0 && countryNewer.getTimestamp() == 0 &&
                    gpcategoryNewer.getTimestamp() == 0) {
                System.out.println("无效行，rowkey：" + rowkey);
                context.getCounter("hainiu_class11", "invalid rowkey").increment(1L);
                return;
            }


            String pkgname = pkgnameNewer.getValue();
            long uptime = Long.parseLong("".equals(uptimeNewer.getValue()) ? "-1" : uptimeNewer.getValue());
            int type = Integer.parseInt("".equals(typeNewer.getValue()) ? "-1" : typeNewer.getValue());
            String country = countryNewer.getValue();
            String gpcategory = gpcategoryNewer.getValue();

            System.out.println("----------------------------");
            System.out.println("aid         :" + aid);
            System.out.println("pkgname     :" + pkgname);
            System.out.println("uptime      :" + uptime);
            System.out.println("type        :" + type);
            System.out.println("country     :" + country);
            System.out.println("gpcategory  :" + gpcategory);


            orcUtil.addAttr(aid, pkgname, uptime, type, country, gpcategory);
            Writable w = orcUtil.serialize();
            context.write(NullWritable.get(), w);
        }

        private CellItemWritable getNewerTimestampData(List<CellItemWritable> list) {
            CellItemWritable itemNewer = new CellItemWritable();

            for(int i = 0; i < list.size(); i++){
                CellItemWritable v = list.get(i);
//				如果是已删除的就跳过
                if(v.isDeleted() && v.getTimestamp() > 0){
                    continue;
                }
                // 把最新版本的数据给 itemNewer
                itemNewer.setName(v.getName());
                itemNewer.setValue(v.getValue());
                itemNewer.setTimestamp(v.getTimestamp());
                itemNewer.setDeleted(v.isDeleted());

                // 跳出
                break;
            }

            return itemNewer;

        }
    }

    @Override
    public Job getJob() throws IOException {
        conf.set("orc.compress" , CompressionKind.SNAPPY.name());
        conf.set("orc.create.index" , "true");

        Job job = Job.getInstance(conf , getJobNameWithTaskId());

        job.setJarByClass(HFileToOrc.class);

        job.setMapperClass(HFileToOrcMapper.class);
        job.setReducerClass(HFileToOrcReducer.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(CellItemWritable.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);

        job.setInputFormatClass(HFileInputFormat.class);
        job.setOutputFormatClass(OrcNewOutputFormat.class);


//		FileInputFormat.addInputPath(job, getFirstJobInputPath());
        FileSystem fs = FileSystem.get(conf);

//		inputpath=/tmp/hbase/input_hfile
//        FileStatus[] listStatus = fs.listStatus(getFirstJobInputPath());
//
//        StringBuilder sb = new StringBuilder();
//
//        for(FileStatus fileStatus : listStatus){
//            // /tmp/hbase/input_hfile/region1
//            String path = fileStatus.getPath().toString();
//
//            if(path.contains(".")){
//                System.out.println(path);
//                break;
//            }
//
//            sb.append(path).append("/cf,");
//
//
//        }
//        sb.deleteCharAt(sb.length() - 1);
//        System.out.println("-------------");
//        System.out.println("inputpaths:" + sb.toString());
        FileInputFormat.addInputPath(job,getFirstJobInputPath());

        // 设置输出目录
        Path outputDir = getOutputPath(getJobNameWithTaskId());
        FileOutputFormat.setOutputPath(job, outputDir);

        return job;
    }

    @Override
    public String getJobName() {
        return "HFileToOrc";
    }
}
