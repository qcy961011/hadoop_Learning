package com.qiao.test.divMaxAndMinSaveToLocal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordWritable implements Writable {

    private int number = 0;
    private Text word = new Text();


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(number);
        word.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        number = in.readInt();
        word.readFields(in);
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public Text getWord() {
        return word;
    }

    public void setWord(Text word) {
        this.word = word;
    }

    @Override
    public String toString() {
        return "number=" + number +
                ", word=" + word ;
    }
}
