/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.aldurd392.bigdatacontest;

import java.io.IOException;
import java.util.ArrayList;

import com.github.aldurd392.bigdatacontest.datatypes.NeighbourhoodMap;
import com.github.aldurd392.bigdatacontest.neighbourhood.NeighbourMapper;
import com.github.aldurd392.bigdatacontest.subgrapher.SubgraphMapper;
import com.github.aldurd392.bigdatacontest.subgrapher.SubgraphReducer;
import com.github.aldurd392.bigdatacontest.datatypes.IntArrayWritable;
import com.github.aldurd392.bigdatacontest.neighbourhood.NeighbourReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main {

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {

        // First round: ------ CountEdges -------
        Job neighbourhoodJob = Job.getInstance();
        neighbourhoodJob.setJobName("Neighbourhood");

        FileInputFormat.addInputPath(neighbourhoodJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(neighbourhoodJob, new Path(args[1]));

        neighbourhoodJob.setMapperClass(NeighbourMapper.class);
        neighbourhoodJob.setReducerClass(NeighbourReducer.class);

        neighbourhoodJob.setInputFormatClass(KeyValueTextInputFormat.class);
        neighbourhoodJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        neighbourhoodJob.setMapOutputKeyClass(IntWritable.class);
        neighbourhoodJob.setMapOutputValueClass(IntWritable.class);
        neighbourhoodJob.setOutputKeyClass(IntArrayWritable.class);
        neighbourhoodJob.setOutputValueClass(NeighbourhoodMap.class);

        ControlledJob neighbourhoodControlledJob = new ControlledJob(neighbourhoodJob, null);

        // Next rounds: ------ Build the subgraph -------
        Job subgraphJob = Job.getInstance();
        subgraphJob.setJobName("Subgrapher");

        FileInputFormat.setInputPaths(subgraphJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(subgraphJob, new Path(args[2]));

        subgraphJob.setInputFormatClass(SequenceFileInputFormat.class);
        subgraphJob.setOutputFormatClass(TextOutputFormat.class);

        subgraphJob.setMapperClass(SubgraphMapper.class);
        subgraphJob.setReducerClass(SubgraphReducer.class);

        subgraphJob.setMapOutputKeyClass(IntArrayWritable.class);
        subgraphJob.setMapOutputValueClass(NeighbourhoodMap.class);
        subgraphJob.setOutputKeyClass(IntArrayWritable.class);
        subgraphJob.setOutputValueClass(NeighbourhoodMap.class);

        ArrayList<ControlledJob> dependencies = new ArrayList<>(1);
        dependencies.add(neighbourhoodControlledJob);
        ControlledJob subgraphControlledJob = new ControlledJob(subgraphJob, dependencies);

        // Job controller -----

        JobControl controller = new JobControl("BigDataContest");

        controller.addJob(neighbourhoodControlledJob);
        controller.addJob(subgraphControlledJob);

        controller.run();
    }
}
