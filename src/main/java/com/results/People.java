package com.results;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class People implements WritableComparable<People> {

    private Text name = new Text();

    private Text sex = new Text();

    private IntWritable age = new IntWritable();

    @Override
    public int compareTo(People o) {
        if("girl".equals(o.sex)){
            return 1;
        } else {
            return -1;
        }
    }


    @Override
    public void write(DataOutput out) throws IOException {
        name.write(out);
        sex.write(out);
        age.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name.readFields(in);
        sex.readFields(in);
        age.readFields(in);
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public Text getSex() {
        return sex;
    }

    public void setSex(Text sex) {
        this.sex = sex;
    }

    public IntWritable getAge() {
        return age;
    }

    public void setAge(IntWritable age) {
        this.age = age;
    }



}
