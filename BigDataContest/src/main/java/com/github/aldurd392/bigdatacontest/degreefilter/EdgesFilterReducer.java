package com.github.aldurd392.bigdatacontest.degreefilter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;

public class EdgesFilterReducer extends Reducer<IntArrayWritable, ShortWritable, IntWritable, IntWritable> {

    public void reduce(IntArrayWritable key, Iterable<ShortWritable> values, Context context) throws IOException, InterruptedException {
    	int sum = 0;

        for (ShortWritable one : values) {
            sum += one.get();
        }
        
        if (sum == 2) {
            IntWritable[] a = (IntWritable[]) key.get();
            context.write(a[0], a[1]);
        }
    }
}
