package com.qiao.test.divMaxAndMinSaveToLocal;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumberWritable implements Writable {

    String number = "";

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(number);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.number = in.readUTF();
    }
}
