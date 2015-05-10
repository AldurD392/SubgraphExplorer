package com.github.aldurd392.bigdatacontest.datatypes;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
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

    @SuppressWarnings("NullableProblems")
    @Override
    public int compareTo(Object o) {
        if (o instanceof IntArrayWritable) {
            IntArrayWritable other = (IntArrayWritable) o;

            Writable[] our_writables = this.get();
            Writable[] other_writables = other.get();

            for (int i = 0; i < other_writables.length; i++) {
                IntWritable w = (IntWritable) our_writables[i];
                IntWritable x = (IntWritable) other_writables[i];

                if (x.get() == w.get()) {
                    continue;
                }

                return x.get() > w.get() ? -1 : 1;
            }
            return 0;
        }

        return 1;
    }
}

