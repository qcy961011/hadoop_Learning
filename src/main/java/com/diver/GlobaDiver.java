package com.diver;

import com.hbase.AvroToHFile.AvroToHFileJob;
import com.hbase.HbaseFileGenerate.FileGenerate;
import com.hbase.HbaseFileGenerate.JobBase;
import com.qiao.test.baseMr.WordMaxJobBase;
import com.qiao.test.teacherHomeWork.diver.JobLinkDiver;
import com.results.PeopleDiver;
import org.apache.hadoop.util.ProgramDriver;


public class GlobaDiver {

    public static void main(String[] args) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();
        try {
            programDriver.addClass("baseWcMax" , WordMaxJobBase.class , "获取一个文件中出现次数最高的单词的任务链");
            programDriver.addClass("maxResult" , JobLinkDiver.class , "学生总成绩排序");
            programDriver.addClass("PeopleNumber" , PeopleDiver.class , "统计不同性别人数");
            programDriver.addClass("orcToHfile" , JobBase.class , "orc文件转hfile文件");
            programDriver.addClass("AvroToHFile" , AvroToHFileJob.class , "Avro文件转hfile文件");

            exitCode = programDriver.run(args);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        System.exit(exitCode);
    }
}
