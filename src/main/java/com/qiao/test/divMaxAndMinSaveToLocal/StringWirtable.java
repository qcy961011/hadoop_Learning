package com.qiao.test.divMaxAndMinSaveToLocal;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringWirtable implements Writable {

    int sum = 0;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readInt();
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return "sum=" + sum ;
    }
}
