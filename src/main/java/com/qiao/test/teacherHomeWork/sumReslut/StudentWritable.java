package com.qiao.test.teacherHomeWork.sumReslut;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StudentWritable implements WritableComparable<StudentWritable> {

    private Text name = new Text();

    private IntWritable result = new IntWritable();

    @Override
    public int compareTo(StudentWritable o) {
        if(this.result.get() > o.getResult().get()){
            return 1;
        } else if(this.result.get() == o.getResult().get()) {
            return 0;
        } else {
            return -1;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        name.write(out);
        result.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name.readFields(in);
        result.readFields(in);
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public IntWritable getResult() {
        return result;
    }

    public void setResult(IntWritable result) {
        this.result = result;
    }


}
