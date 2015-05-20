package com.github.aldurd392.bigdatacontest.datatypes;

import java.util.Comparator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;

public class IntegerValueComparator implements Comparator<IntWritable> {

    Map<IntWritable, Integer> base;

    public IntegerValueComparator(Map<IntWritable, Integer> base) {
        this.base = base;
    }

    @Override
    public int compare(IntWritable o1, IntWritable o2) {
        if (base.get(o1) >= base.get(o2)) {
            return 1;
        } else {
            return -1;
        } // returning 0 would merge keys
    }

}
