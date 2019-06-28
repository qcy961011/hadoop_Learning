package com.hbase.until;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;

/**
 * 由于没有对应的读取Hfile文件的 InputFormat
 * 所以自行封装此类
 * 作为HFile的InputFormat使用
 */


public class HFileInputFormat extends FileInputFormat<ImmutableBytesWritable, Cell> {

    @Override
    public RecordReader<ImmutableBytesWritable, Cell> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new HFileRecordReader((FileSplit) split , context);
    }

    public static class HFileRecordReader extends RecordReader<ImmutableBytesWritable, Cell> {

        /**
         * 读取HFile的Reader
         */
        Reader reader = null;

        /**
         * 扫描器对象
         */
        HFileScanner scanner = null;

        /**
         * 统计记录数
         */
        long count = 0;

        public HFileRecordReader(FileSplit fileSplit, TaskAttemptContext context) throws IOException {
            Configuration configuration = context.getConfiguration();

            FileSystem fs = FileSystem.get(configuration);

            Path path = fileSplit.getPath();

            CacheConfig cacheConfig = new CacheConfig(configuration);

            // 初始化 hbase 的 recorderader
            reader = HFile.createReader(fs, path, cacheConfig, configuration);

            // 获取读取hfile 文件的扫面器
            // 第一个参数： 不缓存
            // 第二个参数： 不随机读写
            scanner = reader.getScanner(false, false);

            // 把扫面器调到首行
            scanner.seekTo();
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            // 如果是首行，直接返回true
            if (count == 0) {
                count++;
                return true;
            }
            // 如果不是首行，执行next找下一个有没有，同时把扫描器进行下移
            // 当调用该方法时，扫面器下移
            boolean hasNext = scanner.next();
            if (hasNext) {
                count++;
            }
            return hasNext;
        }


        /**
         * 该方法用来设置inputKey
         * @return
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
            Cell cell = scanner.getKeyValue();
            ImmutableBytesWritable key = new ImmutableBytesWritable();
            key.set(CellUtil.cloneRow(cell));
            return key;
        }

        /**
         * 该方法用来设置inputValue
         * @return
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public Cell getCurrentValue() throws IOException, InterruptedException {
            return scanner.getKeyValue();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float) count / reader.getEntries();
        }

        @Override
        public void close() throws IOException {
            if(reader != null) {
                reader.close();
            }
        }
    }

}
