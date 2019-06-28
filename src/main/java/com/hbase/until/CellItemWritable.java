/**
 * CellItemWriable.java
 * com.hainiuxy.mapreduce.mrrun.hbase.writable
 * Copyright (c) 2019, 海牛版权所有.
 * @author   潘牛                      
*/

package com.hbase.until;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 封装一个cell对象，并实现按照时间戳降序排序
 * @author   潘牛                      
 * @Date	 2019年4月16日 	 
 */
public class CellItemWritable implements Writable, Comparable<CellItemWritable>{
	/**
	 *  字段名称
	 */
	private String name = "";
	
	/**
	 * 字段值
	 */
	private String value = "";
	
	/**
	 * 所对应的时间戳
	 */
	private long timestamp = 0L;
	
	/**
	 * 字段是否被删除（删除最新版本）
	 */
	private boolean deleted = false;
	
	
	public CellItemWritable(){}
	
	
	
	
	public CellItemWritable(String name, String value, long timestamp, boolean deleted) {
		super();
		this.name = name;
		this.value = value;
		this.timestamp = timestamp;
		this.deleted = deleted;
	}




	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(value);
		out.writeLong(timestamp);
		out.writeBoolean(deleted);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.value = in.readUTF();
		this.timestamp = in.readLong();
		this.deleted = in.readBoolean();
		
	}

	@Override
	public int compareTo(CellItemWritable o) {
		// 默认降序排序
		if(this.timestamp < o.timestamp){
			return 1;
		}else if(this.timestamp == o.timestamp){
			return 0;
		}
		return -1;
		
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public boolean isDeleted() {
		return deleted;
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

	@Override
	public String toString() {
		return "CellItemWriable [name=" + name + ", value=" + value + ", timestamp=" + timestamp + ", deleted="
				+ deleted + "]";
	}
	
	

}

