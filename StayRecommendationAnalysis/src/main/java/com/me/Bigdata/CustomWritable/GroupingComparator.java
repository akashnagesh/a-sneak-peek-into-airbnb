package com.me.Bigdata.CustomWritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {

	protected GroupingComparator() {
		super(CustomKey.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CustomKey cw1 = (CustomKey) w1;
		CustomKey cw2 = (CustomKey) w2;
		return cw1.key.compareTo(cw2.key);
	}

}