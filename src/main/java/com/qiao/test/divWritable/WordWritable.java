package com.qiao.test.divWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordWritable implements Writable {

    private long num = 0;

    private String word = "";

    private Text type = new Text();

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(num);
        out.writeUTF(word);
        type.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        num = in.readLong();
        type.readFields(in);
    }

    public long getNum() {
        return num;
    }

    public void setNum(long num) {
        this.num = num;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Text getType() {
        return type;
    }

    public void setType(Text type) {
        this.type = type;
    }
}
