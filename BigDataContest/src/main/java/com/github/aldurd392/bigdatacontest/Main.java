/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.aldurd392.bigdatacontest;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import CustomTypes.LongWritableArray;
import DegreesFilter.NeighbourReducer;

public class Main {

	public static void main(String[] args) 
		throws IOException, InterruptedException, ClassNotFoundException {
				
		// First job1: CountEdges
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Filter");
		
		job.setMapperClass(NeighbourMapper.class);
		job.setReducerClass(NeighbourReducer.class);
		
		// mapper output types
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		// reducer output types
		job.setOutputKeyClass(LongWritableArray.class);
		job.setOutputValueClass(ShortWritable.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		}
}
