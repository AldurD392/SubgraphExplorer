package com.github.aldurd392.bigdatacontest.degreefilter;

import java.io.IOException;

import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.mapreduce.Mapper;

import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;

public class EdgesCountMapper extends Mapper<IntArrayWritable, ShortWritable, IntArrayWritable, ShortWritable> {

	public void map(IntArrayWritable key, ShortWritable value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}	
}
