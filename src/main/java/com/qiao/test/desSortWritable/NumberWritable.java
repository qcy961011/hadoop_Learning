package com.qiao.test.desSortWritable;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumberWritable implements WritableComparable<NumberWritable> {

    private long num = 0L;

    @Override
    public int compareTo(NumberWritable o) {
        return -ascSort(o);
    }

    private int ascSort(NumberWritable o) {
        if (this.num > o.num) {
            return 1;
        } else if (this.num == o.num){
            return 0;
        }
        return -1;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(num);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        num = in.readLong();
    }

    public long getNum() {
        return num;
    }

    public void setNum(long num) {
        this.num = num;
    }
}
