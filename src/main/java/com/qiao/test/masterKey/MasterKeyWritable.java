package com.qiao.test.masterKey;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MasterKeyWritable implements WritableComparable<MasterKeyWritable> {

    private Text slaveKey = new Text();

    private LongWritable matserKey = new LongWritable();


    @Override
    public void write(DataOutput out) throws IOException {
        slaveKey.write(out);
        matserKey.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        slaveKey.readFields(in);
        matserKey.readFields(in);
    }

    /**
     * 实现二次排序的逻辑
     * 先按照主关键字排序，如果主关键字一样，再按照次关键字拍
     * @param o
     * @return
     */

    @Override
    public int compareTo(MasterKeyWritable o) {
        int firstRedult = this.slaveKey.compareTo(o.slaveKey);
        if (firstRedult == 0) {
            return -this.matserKey.compareTo(o.matserKey);
        }
        return firstRedult;
    }

    public Text getSlaveKey() {
        return slaveKey;
    }

    public void setSlaveKey(Text slaveKey) {
        this.slaveKey = slaveKey;
    }

    public LongWritable getMatserKey() {
        return matserKey;
    }

    public void setMatserKey(LongWritable matserKey) {
        this.matserKey = matserKey;
    }

    @Override
    public String toString() {
        return  slaveKey.toString() + "\t" + matserKey.get();
    }
}
