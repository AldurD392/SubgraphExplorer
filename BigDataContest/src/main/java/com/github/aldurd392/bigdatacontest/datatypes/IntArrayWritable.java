package com.github.aldurd392.bigdatacontest.datatypes;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntArrayWritable extends ArrayWritable implements WritableComparable {
	 
    public IntArrayWritable() {
        super(IntWritable.class);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (String s : super.toStrings()) {
            sb.append(s).append(" ");
        }

        return "[" + sb.toString() + "]";
    }

    @Override
    public int compareTo(Object o) {
        if (o == null) {
            return -1;
        }

        if (o instanceof IntArrayWritable) {
            return 0;
        }

        return 0;
    }
}

