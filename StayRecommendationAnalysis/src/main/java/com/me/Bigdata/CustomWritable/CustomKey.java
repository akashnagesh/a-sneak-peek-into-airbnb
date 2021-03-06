package com.me.Bigdata.CustomWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomKey implements WritableComparable<CustomKey> {
	Integer key;
	Double id;

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(key);
		out.writeDouble(id);

	}

	public CustomKey() {
		// TODO Auto-generated constructor stub
	}

	public CustomKey(int key, double id) {
		this.key = key;
		this.id = id;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		key = in.readInt();
		id = in.readDouble();
	}

	public Integer getKey() {
		return key;
	}

	public void setKey(Integer key) {
		this.key = key;
	}

	@Override
	public int compareTo(CustomKey o) {
		// TODO Auto-generated method stub
		return -1 * key.compareTo(o.key);
	}

	public Double getId() {
		return id;
	}

	public void setId(Double id) {
		this.id = id;
	}

}
